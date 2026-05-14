"""
Regression tests for the amount_usd double-conversion bug.

Bug history:
  ingest.py converts tokens->USD at write time and stores borrows.amount_usd.
  detect.py used to pass amount_usd through to_usd() again, multiplying
  the value by the static price a second time.

  A $10,000 WETH borrow became $10,000 x 3,500 = $35,000,000.

The fix ensures detect.py treats amount_usd as already-USD throughout.

These tests prove the fix holds.
"""
import sqlite3
import tempfile
import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent))

import detect

# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(monkeypatch):
    """Create an in-memory-ish DB with the borrows schema, patch DB_PATH."""
    db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = Path(db.name)
    monkeypatch.setattr(detect, "DB_PATH", db_path)

    conn = sqlite3.connect(db.name)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS borrows (
            id TEXT PRIMARY KEY,
            wallet TEXT,
            asset TEXT,
            amount_usd REAL,
            timestamp INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT,
            rule_id TEXT,
            fired_at TEXT,
            severity TEXT,
            detail TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
            address TEXT PRIMARY KEY,
            risk_score REAL DEFAULT 0,
            last_scored TEXT,
            is_contract INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS anomalies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            borrow_id TEXT,
            wallet TEXT,
            tx_hash TEXT,
            asset TEXT,
            amount_usd REAL,
            zscore REAL,
            risk_score REAL,
            risk_adjusted REAL,
            risk_flag TEXT,
            health_factor REAL,
            liquidation_price REAL,
            liq_gap_pct REAL,
            narrative TEXT,
            detected_at TEXT
        )
    """)
    conn.commit()
    yield conn
    conn.close()


def insert_borrow(conn, bid, wallet, asset, amount_usd, ts=1700000000):
    conn.execute(
        "INSERT OR IGNORE INTO borrows (id, wallet, asset, amount_usd, timestamp) "
        "VALUES (?, ?, ?, ?, ?)",
        (bid, wallet, asset, amount_usd, ts),
    )
    conn.commit()


# ── Test 1: load_borrows_by_asset reads amount_usd directly ──────────

def test_load_borrows_no_double_conversion(tmp_db):
    """
    A $10,000 WETH borrow must stay $10,000 after load_borrows_by_asset().
    The old bug multiplied it by WETH price ($3,500) -> $35M.
    """
    insert_borrow(tmp_db, "evt:1:0xaaa", "0xwallet1", "WETH", 10000.0)
    insert_borrow(tmp_db, "evt:2:0xbbb", "0xwallet1", "WETH", 10000.0)
    insert_borrow(tmp_db, "evt:3:0xccc", "0xwallet2", "WETH", 10000.0)

    by_asset = detect.load_borrows_by_asset()

    assert "WETH" in by_asset
    for event in by_asset["WETH"]:
        assert event["amount_usd"] == pytest.approx(10000.0), (
            f"amount_usd should remain $10,000 but got ${event['amount_usd']:,.0f} — "
            "to_usd() was called again (double conversion)"
        )


# ── Test 2: rule_large_single uses amount_usd directly ──────────────

def test_rule_large_single_no_double_conversion(tmp_db, monkeypatch):
    """
    rule_large_single must compare amount_usd directly, not pass it
    through to_usd().  $55M single borrow must flag as is.
    """
    # Patch min_usd threshold low enough for this test
    monkeypatch.setitem(detect.RULES["LARGE_SINGLE"], "min_usd", 50_000_000)

    recent_borrows = [
        {"id": "big:1", "wallet": "0xwhale", "asset": "WETH",
         "amount_usd": 55_000_000, "timestamp": 1700000000},
    ]

    result = detect.rule_large_single(tmp_db, "0xwhale", recent_borrows)

    assert result is not None, "Should have fired LARGE_SINGLE for $55M"
    assert result["rule_id"] == "LARGE_SINGLE"
    assert "55,000,000" in result["detail"], (
        "Detail should show $55M — if it shows $55M x 3500 it's double-converted"
    )


# ── Test 3: to_usd is a pure function, not in the data path ─────────

def test_to_usd_not_in_borrow_data_path():
    """
    Verify that load_borrows_by_asset and rule_large_single source code
    do not contain to_usd() calls.  This is a static regression guard.
    """
    import inspect

    # load_borrows_by_asset must not call to_usd
    src_load = inspect.getsource(detect.load_borrows_by_asset)
    assert "to_usd" not in src_load, (
        "load_borrows_by_asset must not call to_usd() — double conversion bug"
    )

    # rule_large_single must not call to_usd
    src_rule = inspect.getsource(detect.rule_large_single)
    assert "to_usd" not in src_rule, (
        "rule_large_single must not call to_usd() — double conversion bug"
    )

    # rule_position_build must not call to_usd
    src_pos = inspect.getsource(detect.rule_position_build)
    assert "to_usd" not in src_pos, (
        "rule_position_build must not call to_usd() — double conversion bug"
    )

    # rule_multi_asset must not call to_usd
    src_multi = inspect.getsource(detect.rule_multi_asset)
    assert "to_usd" not in src_multi, (
        "rule_multi_asset must not call to_usd() — double conversion bug"
    )

    # rule_rapid_repeat must not call to_usd
    src_rapid = inspect.getsource(detect.rule_rapid_repeat)
    assert "to_usd" not in src_rapid, (
        "rule_rapid_repeat must not call to_usd() — double conversion bug"
    )


# ── Test 4: $10k WETH remains $10k through z-score path ────────────

def test_zscore_path_preserves_usd(tmp_db):
    """
    In the legacy z-score path, amounts fed into zscore() must be the
    raw amount_usd — not amount_usd * price again.

    With WETH at $3,500, a $10,000 borrow would become $35,000,000
    if double-converted.
    """
    # Insert 5 WETH borrows around $10k so z-score is low
    for i in range(5):
        insert_borrow(tmp_db, f"z{i}:0x{i}", f"0xw{i}", "WETH", 10_000.0, ts=1700000000 + i)

    by_asset = detect.load_borrows_by_asset()
    events = by_asset["WETH"]
    amounts = [e["amount_usd"] for e in events]

    # All amounts should be ~$10,000, not ~$35,000,000
    for amt in amounts:
        assert amt < 20_000, (
            f"Got ${amt:,.0f} — expected ~$10,000. "
            "Double conversion: amount_usd was multiplied by WETH price again."
        )

    # zscore of identical values should be ~0
    z = detect.zscore(amounts, 10_000.0)
    assert abs(z) < 0.01, f"z-score should be near 0, got {z:.4f}"


# ── Test 5: amount_usd round-trip integrity ────────────────────────

def test_amount_usd_roundtrip(tmp_db):
    """
    Write known USD values, read them via load_borrows_by_asset,
    verify no silent multiplication.
    """
    test_cases = [
        ("0xstables", "USDC", 500_000),
        ("0xweth_wallet", "WETH", 100_000),
        ("0xwbtc_wallet", "WBTC", 250_000),
        ("0xmixed", "LINK", 75_000),
    ]
    for i, (wallet, asset, usd) in enumerate(test_cases):
        insert_borrow(tmp_db, f"rt{i}:0x{i}", wallet, asset, float(usd), ts=1700000000)

    by_asset = detect.load_borrows_by_asset()
    for asset, events in by_asset.items():
        for event in events:
            expected = next(
                u for w, a, u in test_cases if a == asset
            )
            assert event["amount_usd"] == pytest.approx(expected), (
                f"Mismatch for {asset}: expected ${expected:,.0f}, "
                f"got ${event['amount_usd']:,.0f}"
            )
