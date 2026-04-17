# detect.py
import sqlite3
import logging
import httpx
from datetime import datetime, timezone
from pathlib import Path
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "blockbridge.db"

OLLAMA_URL   = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen2.5:7b"

# ─── PRICE MAP ──────────────────────────────────────────────────────────────
COINGECKO_PRICES = {
    "WETH":   3500,  "ETH":    3500,
    "WBTC":  65000,  "BTC":   65000,
    "USDC":      1,  "USDT":      1,
    "DAI":       1,  "GHO":       1,
    "USDe":      1,  "PYUSD":     1,
    "LINK":     18,  "AAVE":    120,
    "MATIC":   0.8,  "CRV":    0.45,
    "MKR":    2200,  "SNX":     3.5,
    "UNI":       8,  "LDO":       2,
    "RPL":      20,  "cbETH":  3700,
    "wstETH": 3800,  "rETH":   3700,
}

def get_usd_price(asset: str) -> float:
    return COINGECKO_PRICES.get(asset.upper(), 1.0)

def to_usd(amount: float, asset: str) -> float:
    return amount * get_usd_price(asset)

# ─── RULE CONFIG ─────────────────────────────────────────────────────────────
# Each rule can be toggled. Thresholds calibrated against your actual data.
RULES = {
    "MULTI_ASSET":    {"enabled": True,  "min_assets": 5,    "window_hours": 24},
    "POSITION_BUILD": {"enabled": True,  "min_borrows": 10,  "growth_pct": 50},
    "RAPID_REPEAT":   {"enabled": True,  "min_borrows": 5,   "window_minutes": 60},
    "LARGE_SINGLE":   {"enabled": True,  "min_usd": 50000000},
    "BLOCK_COORD":    {"enabled": True,  "min_wallets": 3},
}

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def tx_hash_from_borrow_id(borrow_id: str):
    parts = borrow_id.split(":")
    if len(parts) >= 3 and parts[2].startswith("0x"):
        return parts[2]
    return None

def fetch_current_health_factor(wallet: str):
    return None  # stub — wire Aave API later

def adjust_risk_with_hf(base_risk: float, hf):
    if hf is None:
        return base_risk, "UNKNOWN"
    if hf <= 1.2:
        return min(10.0, round(base_risk + 3.0, 2)), "NEAR_LIQUIDATION"
    if hf <= 1.4:
        return min(10.0, round(base_risk + 1.5, 2)), "ELEVATED"
    if hf >= 2.0:
        return max(0.0, round(base_risk - 2.0, 2)), "HEALTHY"
    return base_risk, "MONITOR"

def already_signal_exists(conn, wallet: str, rule_id: str, window_hours: int = 1) -> bool:
    """Prevent duplicate signals within window."""
    cutoff = datetime.utcnow().timestamp() - (window_hours * 3600)
    cutoff_str = datetime.utcfromtimestamp(cutoff).isoformat()
    row = conn.execute("""
        SELECT id FROM signals
        WHERE wallet = ? AND rule_id = ? AND fired_at > ?
    """, (wallet, rule_id, cutoff_str)).fetchone()
    return row is not None

def save_signal(conn, wallet: str, rule_id: str, severity: str, detail: str):
    conn.execute("""
        INSERT INTO signals (wallet, rule_id, fired_at, severity, detail)
        VALUES (?, ?, ?, ?, ?)
    """, (wallet, rule_id, datetime.utcnow().isoformat(), severity, detail))

def update_wallet_score(conn, wallet: str, score: float):
    conn.execute("""
        UPDATE wallets SET risk_score = ?, last_scored = ?
        WHERE address = ?
    """, (score, datetime.utcnow().isoformat(), wallet))

# ─── RULES ENGINE ────────────────────────────────────────────────────────────

def rule_multi_asset(conn, wallet: str) -> dict | None:
    """
    MULTI_ASSET: wallet borrows from 5+ different assets within 24 hours.
    Calibrated: top wallets use 7-11 assets. 5+ is meaningful threshold.
    """
    cfg = RULES["MULTI_ASSET"]
    if not cfg["enabled"]:
        return None

    window_secs = cfg["window_hours"] * 3600
    cutoff = int(datetime.utcnow().timestamp()) - window_secs

    rows = conn.execute("""
        SELECT COUNT(DISTINCT asset) as asset_count,
               COUNT(*) as borrow_count
        FROM borrows
        WHERE wallet = ? AND timestamp > ?
    """, (wallet, cutoff)).fetchone()

    if rows and rows[0] >= cfg["min_assets"]:
        return {
            "rule_id":  "MULTI_ASSET",
            "severity": "HIGH" if rows[0] >= 8 else "MEDIUM",
            "detail":   f"{rows[0]} different assets borrowed in last {cfg['window_hours']}h ({rows[1]} total borrows)"
        }
    return None

def rule_position_build(conn, wallet: str) -> dict | None:
    """
    POSITION_BUILD: borrow amounts increasing significantly over time.
    Uses last 20 borrows per asset. Flags if latest 5 avg > first 5 avg by growth_pct%.
    """
    cfg = RULES["POSITION_BUILD"]
    if not cfg["enabled"]:
        return None

    assets = conn.execute("""
        SELECT DISTINCT asset FROM borrows
        WHERE wallet = ?
    """, (wallet,)).fetchall()

    flagged_assets = []
    for (asset,) in assets:
        rows = conn.execute("""
            SELECT amount_usd, timestamp FROM borrows
            WHERE wallet = ? AND asset = ?
            ORDER BY timestamp ASC
        """, (wallet, asset)).fetchall()

        if len(rows) < cfg["min_borrows"]:
            continue

        amounts = [to_usd(r[0], asset) for r in rows]
        first_avg = np.mean(amounts[:5])
        last_avg  = np.mean(amounts[-5:])

        if first_avg > 0:
            growth = ((last_avg - first_avg) / first_avg) * 100
            if growth >= cfg["growth_pct"]:
                flagged_assets.append(
                    f"{asset} +{growth:.0f}% (${first_avg:,.0f} → ${last_avg:,.0f})"
                )

    if flagged_assets:
        return {
            "rule_id":  "POSITION_BUILD",
            "severity": "HIGH" if len(flagged_assets) >= 2 else "MEDIUM",
            "detail":   f"Growing position detected: {', '.join(flagged_assets)}"
        }
    return None

def rule_rapid_repeat(conn, wallet: str) -> dict | None:
    """
    RAPID_REPEAT: 5+ borrows within 60 minutes.
    Calibrated: 0xa508b7 has 1557 borrows — this is bot behaviour.
    """
    cfg = RULES["RAPID_REPEAT"]
    if not cfg["enabled"]:
        return None

    window_secs = cfg["window_minutes"] * 60
    cutoff = int(datetime.utcnow().timestamp()) - window_secs

    row = conn.execute("""
        SELECT COUNT(*) FROM borrows
        WHERE wallet = ? AND timestamp > ?
    """, (wallet, cutoff)).fetchone()

    if row and row[0] >= cfg["min_borrows"]:
        return {
            "rule_id":  "RAPID_REPEAT",
            "severity": "HIGH" if row[0] >= 20 else "MEDIUM",
            "detail":   f"{row[0]} borrows in last {cfg['window_minutes']} minutes"
        }
    return None

def rule_large_single(conn, wallet: str, recent_borrows: list) -> dict | None:
    """
    LARGE_SINGLE: any single borrow > $1M USD.
    """
    cfg = RULES["LARGE_SINGLE"]
    if not cfg["enabled"]:
        return None

    for b in recent_borrows:
        usd = to_usd(b["amount_usd"], b["asset"])
        if usd >= cfg["min_usd"]:
            return {
                "rule_id":  "LARGE_SINGLE",
                "severity": "HIGH" if usd >= 5_000_000 else "MEDIUM",
                "detail":   f"Single borrow of ${usd:,.0f} {b['asset']}"
            }
    return None

def rule_block_coord(conn, recent_borrows: list) -> list:
    """
    BLOCK_COORD: 3+ different wallets borrow in the same block.
    Returns list of (wallet, signal) pairs.
    """
    cfg = RULES["BLOCK_COORD"]
    if not cfg["enabled"]:
        return []

    # Group by block (timestamp as proxy — borrows in same second)
    from collections import defaultdict
    by_block = defaultdict(set)
    by_block_wallets = defaultdict(list)

    for b in recent_borrows:
        by_block[b["timestamp"]].add(b["wallet"])
        by_block_wallets[b["timestamp"]].append(b)

    results = []
    for ts, wallets in by_block.items():
        if len(wallets) >= cfg["min_wallets"]:
            detail = f"{len(wallets)} wallets borrowed in same block (ts={ts})"
            for wallet in wallets:
                results.append((wallet, {
                    "rule_id":  "BLOCK_COORD",
                    "severity": "HIGH",
                    "detail":   detail
                }))
    return results

# ─── NARRATIVE ───────────────────────────────────────────────────────────────

def get_wallet_narrative(wallet: str, signals: list, stats: dict) -> str:
    if not signals:
        return ""

    signal_summary = "\n".join([f"- {s['rule_id']}: {s['detail']}" for s in signals])

    prompt = (
        f"You are a DeFi risk analyst. A wallet has triggered behavioural alerts on Aave V3.\n\n"
        f"Wallet: {wallet[:12]}...\n"
        f"Total borrows: {stats.get('total_borrows', 0)}\n"
        f"Assets used: {stats.get('assets', 'unknown')}\n"
        f"Active since: {stats.get('first_seen', 'unknown')}\n\n"
        f"Signals fired:\n{signal_summary}\n\n"
        f"Write 3 sentences assessing this wallet's behaviour. "
        f"State what the pattern suggests, what the risk is, and what to watch next. "
        f"Be specific. No markdown."
    )
    try:
        r = httpx.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=30
        )
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception as e:
        log.warning("Ollama unavailable: %s", e)
        fired = ", ".join([s["rule_id"] for s in signals])
        return (
            f"Wallet {wallet[:12]}... has triggered {len(signals)} behavioural rules: {fired}. "
            f"Pattern warrants investigation — {stats.get('total_borrows', 0)} total borrows "
            f"across {stats.get('asset_count', 0)} assets detected."
        )

# ─── LEGACY Z-SCORE (kept for backward compat) ───────────────────────────────

def zscore(values: list, target: float) -> float:
    if len(values) < 3:
        return 0.0
    arr  = np.array(values)
    mean = arr.mean()
    std  = arr.std()
    if std == 0:
        return 0.0
    return float((target - mean) / std)

def risk_from_zscore(z: float) -> float:
    ZSCORE_THRESHOLD = 2.0
    if z < ZSCORE_THRESHOLD:
        return 0.0
    capped = min(z, 6.0)
    return round((capped - ZSCORE_THRESHOLD) / (6.0 - ZSCORE_THRESHOLD) * 10.0, 2)

def get_narrative(asset: str, amount_usd: float, z: float, risk: float) -> str:
    prompt = (
        f"You are a DeFi risk analyst monitoring Aave V3. "
        f"A borrow event has been flagged as anomalous. "
        f"Asset: {asset}, Amount: ${amount_usd:,.0f}, "
        f"Z-score: {z:.2f}, Risk score: {risk:.1f}/10. "
        f"Write exactly 2 sentences explaining the risk. Be specific and concise. "
        f"Do not use markdown."
    )
    try:
        r = httpx.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=30
        )
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception as e:
        log.warning("Ollama unavailable: %s", e)
        return (
            f"Borrow of ${amount_usd:,.0f} in {asset} is {z:.1f} standard deviations "
            f"above the 50-event mean. Risk score {risk:.1f}/10 — monitor closely."
        )

def already_flagged(borrow_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    row  = conn.execute(
        "SELECT id FROM anomalies WHERE borrow_id = ?", (borrow_id,)
    ).fetchone()
    conn.close()
    return row is not None

def save_anomaly(record: dict):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO anomalies (
            borrow_id, wallet, tx_hash, asset, amount_usd,
            zscore, risk_score, risk_adjusted, risk_flag,
            health_factor, liquidation_price, liq_gap_pct,
            narrative, detected_at
        ) VALUES (
            :borrow_id, :wallet, :tx_hash, :asset, :amount_usd,
            :zscore, :risk_score, :risk_adjusted, :risk_flag,
            :health_factor, :liquidation_price, :liq_gap_pct,
            :narrative, :detected_at
        )
    """, record)
    conn.commit()
    conn.close()

# ─── MAIN DETECTION ──────────────────────────────────────────────────────────

def load_borrows_by_asset() -> dict:
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT id, wallet, asset, amount_usd, timestamp
        FROM borrows ORDER BY timestamp DESC
    """).fetchall()
    conn.close()

    by_asset = {}
    for row in rows:
        asset = row[2]
        if asset not in by_asset:
            by_asset[asset] = []
        by_asset[asset].append({
            "id":         row[0],
            "wallet":     row[1],
            "asset":      asset,
            "amount_usd": to_usd(row[3], asset),
            "timestamp":  row[4]
        })
    return by_asset

def run_rules_engine():
    """
    Run behavioural rules against all known EOA wallets.
    Skips contracts (is_contract = 1).
    """
    log.info("Starting rules engine")
    conn = sqlite3.connect(DB_PATH)

    # Get all EOA wallets — skip contracts and unknowns
    wallets = conn.execute("""
        SELECT DISTINCT b.wallet
        FROM borrows b
        LEFT JOIN wallets w ON b.wallet = w.address
        WHERE w.is_contract = 0 OR w.is_contract IS NULL
        AND b.wallet IS NOT NULL
    """).fetchall()

    log.info("Running rules against %d wallets", len(wallets))
    signals_fired = 0

    # Get all recent borrows for block coordination check
    recent_all = conn.execute("""
        SELECT id, wallet, asset, amount_usd, timestamp
        FROM borrows
        WHERE timestamp > ?
        ORDER BY timestamp DESC
    """, (int(datetime.utcnow().timestamp()) - 3600,)).fetchall()

    recent_borrows_list = [
        {"id": r[0], "wallet": r[1], "asset": r[2],
         "amount_usd": r[3], "timestamp": r[4]}
        for r in recent_all
    ]

    # Block coordination — runs once across all wallets
    coord_signals = rule_block_coord(conn, recent_borrows_list)
    for wallet, signal in coord_signals:
        if not already_signal_exists(conn, wallet, "BLOCK_COORD", window_hours=1):
            save_signal(conn, wallet, signal["rule_id"],
                       signal["severity"], signal["detail"])
            signals_fired += 1
            log.info("BLOCK_COORD fired for %s", wallet[:12])

    # Per-wallet rules
    for (wallet,) in wallets:
        if not wallet:
            continue

        wallet_signals = []

        # Get recent borrows for this wallet
        recent = conn.execute("""
            SELECT id, wallet, asset, amount_usd, timestamp
            FROM borrows WHERE wallet = ?
            ORDER BY timestamp DESC LIMIT 50
        """, (wallet,)).fetchall()

        recent_borrows = [
            {"id": r[0], "wallet": r[1], "asset": r[2],
             "amount_usd": r[3], "timestamp": r[4]}
            for r in recent
        ]

        # Run each rule
        for rule_fn, kwargs in [
            (rule_multi_asset,   {"conn": conn, "wallet": wallet}),
            (rule_position_build,{"conn": conn, "wallet": wallet}),
            (rule_rapid_repeat,  {"conn": conn, "wallet": wallet}),
            (rule_large_single,  {"conn": conn, "wallet": wallet,
                                  "recent_borrows": recent_borrows}),
        ]:
            try:
                result = rule_fn(**kwargs)
                if result:
                    if not already_signal_exists(conn, wallet,
                                                  result["rule_id"],
                                                  window_hours=1):
                        save_signal(conn, wallet, result["rule_id"],
                                   result["severity"], result["detail"])
                        wallet_signals.append(result)
                        signals_fired += 1
                        log.info("%s fired for %s: %s",
                                 result["rule_id"], wallet[:12], result["detail"])
            except Exception as e:
                log.error("Rule %s failed for %s: %s", rule_fn.__name__, wallet[:12], e)

        # Update wallet risk score based on signals fired
        if wallet_signals:
            severity_scores = {"HIGH": 8.0, "MEDIUM": 5.0, "LOW": 2.0}
            score = min(10.0, sum(
                severity_scores.get(s["severity"], 2.0)
                for s in wallet_signals
            ) / len(wallet_signals) * (1 + len(wallet_signals) * 0.2))
            update_wallet_score(conn, wallet, round(score, 2))

    conn.commit()
    conn.close()
    log.info("Rules engine complete — %d signals fired", signals_fired)
    return signals_fired

def run_detection() -> list:
    """
    Legacy single-borrow detection — kept for backward compat.
    Also runs rules engine at end of each cycle.
    """
    log.info("Starting detection cycle")
    by_asset = load_borrows_by_asset()
    flagged  = []

    conn_main = sqlite3.connect(DB_PATH)

    for asset, events in by_asset.items():
        amounts = [e["amount_usd"] for e in events]
        if len(amounts) < 3:
            continue

        for event in events[:10]:
            z    = zscore(amounts, event["amount_usd"])
            risk = risk_from_zscore(z)

            if risk > 0 and not already_flagged(event["id"]):
                # Skip contracts
                wallet = event["wallet"]
                if wallet:
                    w_row = conn_main.execute(
                        "SELECT is_contract FROM wallets WHERE address = ?",
                        (wallet,)
                    ).fetchone()
                    if w_row and w_row[0] == 1:
                        continue  # skip contracts

                log.info("Anomaly — %s $%.0f z=%.2f risk=%.1f",
                         asset, event["amount_usd"], z, risk)

                tx_hash  = tx_hash_from_borrow_id(event["id"])
                hf       = fetch_current_health_factor(wallet)
                risk_adjusted, risk_flag = adjust_risk_with_hf(risk, hf)
                narrative = get_narrative(asset, event["amount_usd"], z, risk)

                record = {
                    "borrow_id":         event["id"],
                    "wallet":            wallet,
                    "tx_hash":           tx_hash,
                    "asset":             asset,
                    "amount_usd":        event["amount_usd"],
                    "zscore":            z,
                    "risk_score":        risk,
                    "risk_adjusted":     risk_adjusted,
                    "risk_flag":         risk_flag,
                    "health_factor":     hf,
                    "liquidation_price": None,
                    "liq_gap_pct":       None,
                    "narrative":         narrative,
                    "detected_at":       datetime.utcnow().isoformat()
                }
                save_anomaly(record)
                flagged.append(record)

    conn_main.close()

    # Run behavioural rules engine
    run_rules_engine()

    log.info("Detection complete — %d anomalies flagged", len(flagged))
    return flagged

if __name__ == "__main__":
    results = run_detection()
    print(f"\n{len(results)} anomalies detected")

    # Show signals
    conn = sqlite3.connect(DB_PATH)
    signals = conn.execute("""
        SELECT wallet, rule_id, severity, detail, fired_at
        FROM signals
        ORDER BY fired_at DESC
        LIMIT 20
    """).fetchall()
    conn.close()

    if signals:
        print(f"\nLatest signals:")
        for s in signals:
            print(f"  {s[2]:6s}  {s[1]:15s}  {s[0][:12]}...  {s[3][:60]}")
    else:
        print("No signals fired yet")
