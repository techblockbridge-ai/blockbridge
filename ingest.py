# ingest.py
import sqlite3
import httpx
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "blockbridge.db"

SUBGRAPH_URL = "https://gateway.thegraph.com/api/subgraphs/id/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"

BORROW_QUERY = """
{
  borrows(
    first: 50
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    user { id }
    reserve { symbol underlyingAsset decimals }
    amount
    timestamp
  }
}
"""

# ─── COINGECKO PRICE FETCHING ───────────────────────────────────────────────

# Map Aave reserve symbols → CoinGecko IDs
SYMBOL_TO_COINGECKO = {
    "WETH":   "ethereum",       "ETH":    "ethereum",
    "WBTC":   "wrapped-bitcoin","BTC":    "bitcoin",
    "USDC":   "usd-coin",       "USDT":   "tether",
    "DAI":    "dai",            "GHO":    "gho",
    "USDe":   "ethena-usde",    "PYUSD":  "paypal-usd",
    "LINK":   "chainlink",      "AAVE":   "aave",
    "MATIC":  "matic-network",  "CRV":    "curve-dao-token",
    "MKR":    "maker",          "SNX":    "havven",
    "UNI":    "uniswap",        "LDO":    "lido-dao",
    "RPL":    "rocket-pool",    "cbETH":  "coinbase-wrapped-staked-eth",
    "wstETH": "wrapped-steth",  "rETH":   "rocket-pool-eth",
}

# Static fallback prices — used ONLY when CoinGecko is unreachable
FALLBACK_PRICES = {
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

# Module-level price cache — refreshed each ingest cycle
_price_cache: dict[str, float] = {}

def fetch_live_prices(symbols: list[str]) -> dict[str, float]:
    """
    Batch-fetch USD prices from CoinGecko free API.
    Returns {SYMBOL: price_usd}. Falls back to static map on failure.
    """
    # Deduplicate and map to CoinGecko IDs
    cg_ids = {}
    for sym in set(symbols):
        cg_id = SYMBOL_TO_COINGECKO.get(sym.upper())
        if cg_id:
            cg_ids[sym.upper()] = cg_id

    if not cg_ids:
        log.warning("No CoinGecko IDs for symbols: %s — using fallback", symbols)
        return {s: FALLBACK_PRICES.get(s.upper(), 1.0) for s in symbols}

    ids_param = ",".join(set(cg_ids.values()))
    try:
        r = httpx.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": ids_param, "vs_currencies": "usd"},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()

        # Reverse-map: CoinGecko ID → symbol(s)
        id_to_syms: dict[str, list[str]] = {}
        for sym, cg_id in cg_ids.items():
            id_to_syms.setdefault(cg_id, []).append(sym)

        prices = {}
        for cg_id, syms in id_to_syms.items():
            usd = data.get(cg_id, {}).get("usd")
            for sym in syms:
                if usd is not None:
                    prices[sym] = float(usd)
                else:
                    prices[sym] = FALLBACK_PRICES.get(sym, 1.0)
                    log.warning("CoinGecko missing price for %s (%s) — fallback $%.2f",
                                sym, cg_id, prices[sym])

        log.info("Live prices fetched for %d assets from CoinGecko", len(prices))
        return prices

    except Exception as e:
        log.warning("CoinGecko fetch failed: %s — using fallback prices", e)
        return {s: FALLBACK_PRICES.get(s.upper(), 1.0) for s in symbols}


def get_price(symbol: str) -> float:
    """Get price from current cycle's cache, or fallback."""
    return _price_cache.get(symbol.upper(), FALLBACK_PRICES.get(symbol.upper(), 1.0))


# ─── DATABASE ────────────────────────────────────────────────────────────────

def ensure_column(conn, table, column, ddl):
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        log.info("Migration: added column %s.%s", table, column)

def init_db():
    conn = sqlite3.connect(DB_PATH)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS borrows (
            id          TEXT PRIMARY KEY,
            wallet      TEXT,
            asset       TEXT,
            amount_raw  TEXT,
            amount_usd  REAL,
            decimals    INTEGER,
            timestamp   INTEGER,
            ingested_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            borrow_id         TEXT,
            wallet            TEXT,
            tx_hash           TEXT,
            asset             TEXT,
            amount_usd        REAL,
            zscore            REAL,
            risk_score        REAL,
            risk_adjusted     REAL,
            risk_flag         TEXT,
            health_factor     REAL,
            liquidation_price REAL,
            liq_gap_pct       REAL,
            narrative         TEXT,
            detected_at       TEXT
        )
    """)

    # ─── Config table (Fix 2: persisted detection parameters) ───
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key   TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    # Seed defaults if empty
    for key, default in [
        ("zscore_threshold", "2.0"),
        ("rolling_window",   "50"),
        ("min_borrow_size",  "0"),
    ]:
        conn.execute(
            "INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)",
            (key, default)
        )

    # Safe migrations for existing DBs
    ensure_column(conn, "borrows",   "amount_tokens",     "amount_tokens REAL")
    ensure_column(conn, "borrows",   "price_at_ingest",   "price_at_ingest REAL")
    ensure_column(conn, "borrows",   "price_source",      "price_source TEXT")
    ensure_column(conn, "borrows",   "protocol",          "protocol TEXT DEFAULT 'aave-v3'")
    ensure_column(conn, "anomalies", "wallet",            "wallet TEXT")
    ensure_column(conn, "anomalies", "tx_hash",           "tx_hash TEXT")
    ensure_column(conn, "anomalies", "risk_adjusted",     "risk_adjusted REAL")
    ensure_column(conn, "anomalies", "risk_flag",         "risk_flag TEXT")
    ensure_column(conn, "anomalies", "health_factor",     "health_factor REAL")
    ensure_column(conn, "anomalies", "liquidation_price", "liquidation_price REAL")
    ensure_column(conn, "anomalies", "liq_gap_pct",       "liq_gap_pct REAL")

    # ─── Backfill: convert legacy rows where amount_usd is actually token amount ───
    # If amount_tokens is NULL but amount_usd is populated, the row is legacy.
    # Copy the token amount across, then recompute USD with fallback prices.
    legacy = conn.execute("""
        SELECT id, asset, amount_usd FROM borrows
        WHERE amount_tokens IS NULL AND amount_usd IS NOT NULL
    """).fetchall()
    if legacy:
        log.info("Backfilling %d legacy rows: amount_tokens + price_at_ingest", len(legacy))
        for row_id, asset, token_amount in legacy:
            price = FALLBACK_PRICES.get(asset.upper(), 1.0)
            conn.execute("""
                UPDATE borrows
                SET amount_tokens   = ?,
                    amount_usd      = ? * ?,
                    price_at_ingest = ?,
                    price_source    = 'backfill_fallback'
                WHERE id = ?
            """, (token_amount, token_amount, price, price, row_id))
        log.info("Backfill complete — %d rows updated with fallback prices", len(legacy))

    conn.commit()
    conn.close()
    log.info("Database initialised at %s", DB_PATH)

def fetch_borrows(api_key: str) -> list[dict]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        r = httpx.post(
            SUBGRAPH_URL,
            json={"query": BORROW_QUERY},
            headers=headers,
            timeout=15
        )
        r.raise_for_status()
        data = r.json()

        if "errors" in data:
            log.error("Subgraph errors: %s", data["errors"])
            return []

        return data.get("data", {}).get("borrows", [])

    except Exception as e:
        log.error("Fetch failed: %s", e)
        return []

def normalise(borrow: dict) -> dict:
    """
    Convert raw subgraph borrow into a DB-ready record.
    Prices are looked up from the module-level _price_cache
    (populated by run_ingest before this is called).
    """
    decimals = int(borrow["reserve"]["decimals"])
    amount_raw = int(borrow["amount"])
    amount_tokens = amount_raw / (10 ** decimals)
    symbol = borrow["reserve"]["symbol"]

    price = get_price(symbol)
    amount_usd = round(amount_tokens * price, 4)
    price_source = "coingecko" if symbol.upper() in _price_cache else "fallback"

    return {
        "id":              borrow["id"],
        "wallet":          borrow["user"]["id"],
        "asset":           symbol,
        "amount_raw":      borrow["amount"],
        "amount_tokens":   amount_tokens,
        "amount_usd":      amount_usd,
        "price_at_ingest": price,
        "price_source":    price_source,
        "decimals":        decimals,
        "timestamp":       int(borrow["timestamp"]),
        "ingested_at":     datetime.utcnow().isoformat(),
    }

def save_borrows(records: list[dict]) -> int:
    if not records:
        return 0

    conn = sqlite3.connect(DB_PATH)
    inserted = 0
    for r in records:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO borrows
                (id, wallet, asset, amount_raw, amount_tokens, amount_usd,
                 price_at_ingest, price_source, decimals, timestamp, ingested_at)
                VALUES (:id, :wallet, :asset, :amount_raw, :amount_tokens, :amount_usd,
                        :price_at_ingest, :price_source, :decimals, :timestamp, :ingested_at)
            """, r)
            if conn.total_changes > inserted:
                inserted += 1
        except Exception as e:
            log.error("Insert failed for %s: %s", r["id"], e)

    conn.commit()
    conn.close()
    return inserted

def run_ingest(api_key: str = ""):
    global _price_cache

    log.info("Starting ingest cycle")
    raw = fetch_borrows(api_key)
    if not raw:
        log.warning("No data returned from subgraph")
        return 0

    # Collect unique asset symbols from this batch
    symbols = list({b["reserve"]["symbol"] for b in raw})
    _price_cache = fetch_live_prices(symbols)
    log.info("Price cache: %s", {s: f"${p:,.2f}" for s, p in _price_cache.items()})

    records = [normalise(b) for b in raw]
    inserted = save_borrows(records)
    log.info("Ingest complete — %d new records (prices: %s)", inserted,
             "live" if any(r["price_source"] == "coingecko" for r in records) else "fallback")
    return inserted

if __name__ == "__main__":
    from dotenv import load_dotenv
    import os

    load_dotenv()
    init_db()
    count = run_ingest(os.getenv("GRAPH_API_KEY", ""))
    print(f"Inserted {count} new borrow events")

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT asset, amount_tokens, amount_usd, price_at_ingest, price_source, timestamp FROM borrows ORDER BY timestamp DESC LIMIT 5"
    ).fetchall()
    conn.close()

    print("\nLatest 5 borrows:")
    for row in rows:
        ts = datetime.utcfromtimestamp(row[5]).strftime("%Y-%m-%d %H:%M")
        tokens = row[1] if row[1] is not None else row[2]  # fallback for legacy rows
        print(f"  {row[0]:10s}  {tokens:>15,.4f} tokens  ${row[2]:>12,.2f} USD  "
              f"@${row[3] or 0:>8,.2f}  ({row[4] or 'legacy'})  {ts}")
