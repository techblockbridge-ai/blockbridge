# wallet_intelligence.py
# On-demand wallet intelligence — queries The Graph, caches results
# Never stores historical data. Cache is a performance layer only.

import httpx
import logging
from datetime import datetime
from cachetools import TTLCache
from pathlib import Path
import os

log = logging.getLogger(__name__)

# ─── CACHE ───────────────────────────────────────────────────────────────────
# Dashboard feed: 5 minute TTL — fast, acceptable staleness
# Wallet profile: 10 minute TTL — fresh enough for investigation
# Contract check: 24 hour TTL — contract status never changes

DASHBOARD_CACHE  = TTLCache(maxsize=1000, ttl=300)    # 5 min
PROFILE_CACHE    = TTLCache(maxsize=500,  ttl=600)    # 10 min
CONTRACT_CACHE   = TTLCache(maxsize=5000, ttl=86400)  # 24 hr

SUBGRAPH_URL = "https://gateway.thegraph.com/api/subgraphs/id/Cd2gEDVeqnjBn1hSeqFMitw8Q1iiyV9FYUZkLNRcL87g"

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

def to_usd(amount: float, asset: str) -> float:
    return amount * COINGECKO_PRICES.get(asset.upper(), 1.0)

# ─── THE GRAPH QUERIES ────────────────────────────────────────────────────────

def query_graph(query: str, api_key: str = "") -> dict:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        r = httpx.post(
            SUBGRAPH_URL,
            json={"query": query},
            headers=headers,
            timeout=15
        )
        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            log.error("Graph errors: %s", data["errors"])
            return {}
        return data.get("data", {})
    except Exception as e:
        log.error("Graph query failed: %s", e)
        return {}

def fetch_wallet_borrows(wallet: str, api_key: str = "", limit: int = 100) -> list:
    """Fetch all borrows for a wallet from The Graph. No storage."""
    cache_key = f"borrows:{wallet}:{limit}"
    if cache_key in PROFILE_CACHE:
        return PROFILE_CACHE[cache_key]

    query = f"""
    {{
      borrows(
        first: {limit}
        orderBy: timestamp
        orderDirection: desc
        where: {{ user: "{wallet.lower()}" }}
      ) {{
        id
        reserve {{ symbol decimals }}
        amount
        timestamp
      }}
    }}
    """
    data = query_graph(query, api_key)
    borrows = data.get("borrows", [])

    result = []
    for b in borrows:
        decimals   = int(b["reserve"]["decimals"])
        amount_raw = int(b["amount"])
        amount     = amount_raw / (10 ** decimals)
        asset      = b["reserve"]["symbol"]
        result.append({
            "id":        b["id"],
            "asset":     asset,
            "amount":    amount,
            "amount_usd": to_usd(amount, asset),
            "timestamp": int(b["timestamp"]),
            "datetime":  datetime.utcfromtimestamp(int(b["timestamp"])).strftime("%Y-%m-%d %H:%M")
        })

    PROFILE_CACHE[cache_key] = result
    return result

def check_is_contract(address: str) -> dict:
    """Check if address is contract via Blockscout. 24hr cache."""
    if address in CONTRACT_CACHE:
        return CONTRACT_CACHE[address]
    try:
        r = httpx.get(
            f"https://eth.blockscout.com/api/v2/addresses/{address}",
            timeout=8
        )
        data = r.json()
        result = {
            "is_contract":   data.get("is_contract", False),
            "contract_name": data.get("name") or None,
            "ens":           data.get("ens_domain_name") or None
        }
        CONTRACT_CACHE[address] = result
        return result
    except Exception:
        return {"is_contract": False, "contract_name": None, "ens": None}

def build_wallet_profile(wallet: str, api_key: str = "") -> dict:
    """
    Build complete wallet intelligence profile on demand.
    Queries The Graph live. Cached for 10 minutes.
    """
    cache_key = f"profile:{wallet}"
    if cache_key in PROFILE_CACHE:
        log.info("Profile cache hit: %s", wallet[:12])
        return PROFILE_CACHE[cache_key]

    log.info("Building wallet profile: %s", wallet[:12])
    borrows = fetch_wallet_borrows(wallet, api_key)

    # Check contract status via Blockscout (cached 24hr)
    contract_info = check_is_contract(wallet)

    if not borrows:
        profile = {
            "wallet":          wallet,
            "total_borrows":   0,
            "total_usd":       0,
            "assets":          [],
            "asset_count":     0,
            "first_borrow":    None,
            "last_borrow":     None,
            "borrow_history":  [],
            "patterns":        [],
            "error":           "No borrow history found"
        }
        PROFILE_CACHE[cache_key] = profile
        return profile

    # Aggregate
    total_usd  = sum(b["amount_usd"] for b in borrows)
    assets     = list(dict.fromkeys(b["asset"] for b in borrows))  # ordered unique
    first      = min(borrows, key=lambda x: x["timestamp"])
    last       = max(borrows, key=lambda x: x["timestamp"])

    # Per-asset breakdown
    asset_breakdown = {}
    for b in borrows:
        a = b["asset"]
        if a not in asset_breakdown:
            asset_breakdown[a] = {"count": 0, "total_usd": 0, "amounts": []}
        asset_breakdown[a]["count"]     += 1
        asset_breakdown[a]["total_usd"] += b["amount_usd"]
        asset_breakdown[a]["amounts"].append(b["amount_usd"])

    # Detect patterns — observable facts, not alerts
    patterns = []

    if len(assets) >= 5:
        patterns.append({
            "id":     "MULTI_ASSET",
            "label":  f"Multi-asset borrower",
            "detail": f"Borrows across {len(assets)} different assets",
            "severity": "HIGH" if len(assets) >= 8 else "MEDIUM"
        })

    # Position building — is latest borrow larger than earliest?
    for asset, data in asset_breakdown.items():
        if len(data["amounts"]) >= 5:
            first_avg = sum(data["amounts"][:3]) / 3
            last_avg  = sum(data["amounts"][-3:]) / 3
            if first_avg > 0:
                growth = ((last_avg - first_avg) / first_avg) * 100
                if growth >= 50:
                    patterns.append({
                        "id":     "POSITION_BUILD",
                        "label":  f"Position building — {asset}",
                        "detail": f"Average borrow size up {growth:.0f}% over time",
                        "severity": "HIGH" if growth >= 200 else "MEDIUM"
                    })

    # Borrow frequency
    if len(borrows) >= 2:
        time_span_days = (last["timestamp"] - first["timestamp"]) / 86400
        if time_span_days > 0:
            freq = len(borrows) / time_span_days
            if freq >= 10:
                patterns.append({
                    "id":     "HIGH_FREQUENCY",
                    "label":  "High frequency borrower",
                    "detail": f"{freq:.1f} borrows per day average",
                    "severity": "MEDIUM"
                })

    profile = {
        "wallet":          wallet,
        "total_borrows":   len(borrows),
        "total_usd":       round(total_usd, 2),
        "assets":          assets,
        "asset_count":     len(assets),
        "asset_breakdown": asset_breakdown,
        "first_borrow":    first["datetime"],
        "last_borrow":     last["datetime"],
        "borrow_history":  borrows[:20],  # last 20 for display
        "patterns":        patterns,
        "is_contract":   contract_info["is_contract"],
        "contract_name": contract_info["contract_name"],
        "ens":           contract_info["ens"],
        "cached_at":       datetime.utcnow().isoformat()
    }

    PROFILE_CACHE[cache_key] = profile
    return profile

def get_active_wallets(api_key: str = "", limit: int = 50) -> list:
    """
    Get recently active wallets from The Graph.
    Used for dashboard feed. 5 min cache.
    """
    cache_key = f"active_wallets:{limit}"
    if cache_key in DASHBOARD_CACHE:
        return DASHBOARD_CACHE[cache_key]

    query = f"""
    {{
      borrows(
        first: {limit}
        orderBy: timestamp
        orderDirection: desc
      ) {{
        user {{ id }}
        reserve {{ symbol }}
        amount
        timestamp
      }}
    }}
    """
    data    = query_graph(query, api_key)
    borrows = data.get("borrows", [])

    # Deduplicate wallets, keep most recent activity
    seen    = {}
    for b in borrows:
        wallet = b["user"]["id"]
        if wallet not in seen:
            seen[wallet] = {
                "wallet":    wallet,
                "last_asset": b["reserve"]["symbol"],
                "last_seen": datetime.utcfromtimestamp(
                    int(b["timestamp"])
                ).strftime("%Y-%m-%d %H:%M")
            }

    result = list(seen.values())
    DASHBOARD_CACHE[cache_key] = result
    return result
