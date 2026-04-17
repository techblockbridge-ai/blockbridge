# api.py
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import uvicorn

from ingest import init_db, run_ingest
from detect import run_detection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "blockbridge.db"

app = FastAPI(title="BlockBridge API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def risk_label(score: float) -> str:
    if score is None:
        return "LOW"
    if score >= 7:
        return "HIGH"
    if score >= 3:
        return "MEDIUM"
    return "LOW"

def ingest_and_detect():
    log.info("Scheduled cycle starting")
    try:
        from dotenv import load_dotenv
        import os
        load_dotenv()
        run_ingest(os.getenv("GRAPH_API_KEY", ""))
        run_detection()
    except Exception as e:
        log.error("Scheduled cycle failed: %s", e)

scheduler = BackgroundScheduler()
scheduler.add_job(ingest_and_detect, "interval", seconds=60, id="ingest_detect")

@app.on_event("startup")
def startup():
    init_db()
    ingest_and_detect()
    scheduler.start()
    log.info("Scheduler started — polling every 60s")

@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown()

@app.get("/api/anomalies")
def get_anomalies(limit: int = 50):
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT a.id, a.borrow_id, COALESCE(a.wallet, b.wallet) as wallet,
               a.tx_hash, a.asset, a.amount_usd,
               a.zscore, a.risk_score, a.risk_adjusted, a.risk_flag,
               a.health_factor, a.liquidation_price, a.liq_gap_pct,
               a.narrative, a.detected_at,
               COALESCE(w.is_contract, -1) as is_contract,
               w.contract_name, w.ens_name
        FROM anomalies a
        LEFT JOIN borrows b ON a.borrow_id = b.id
        LEFT JOIN wallets w ON COALESCE(a.wallet, b.wallet) = w.address
        ORDER BY a.detected_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()

    return [
        {
            "id":                r[0],
            "borrow_id":         r[1],
            "wallet":            r[2],
            "tx_hash":           r[3],
            "asset":             r[4],
            "amount_usd":        r[5],
            "zscore":            r[6],
            "risk_score":        r[7],
            "risk_adjusted":     r[8],
            "risk_flag":         r[9],
            "health_factor":     r[10],
            "liquidation_price": r[11],
            "liq_gap_pct":       r[12],
            "narrative":         r[13],
            "detected_at":       r[14],
            # Use risk_adjusted if available, fall back to risk_score
            "risk_label":     risk_label(r[8] if r[8] is not None else r[7]),
            "is_contract":    r[15],
            "contract_name":  r[16],
            "ens_name":       r[17]
        }
        for r in rows
    ]

@app.get("/api/borrows")
def get_borrows(limit: int = 50):
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT id, wallet, asset, amount_usd, timestamp
        FROM borrows
        ORDER BY timestamp DESC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()

    return [
        {
            "id":         r[0],
            "wallet":     r[1][:8] + "..." + r[1][-4:] if r[1] else "unknown",
            "asset":      r[2],
            "amount_usd": r[3],
            "timestamp":  datetime.utcfromtimestamp(r[4]).strftime("%Y-%m-%d %H:%M:%S")
        }
        for r in rows
    ]

@app.get("/api/stats")
def get_stats():
    conn = sqlite3.connect(DB_PATH)
    total_borrows   = conn.execute("SELECT COUNT(*) FROM borrows").fetchone()[0]
    total_anomalies = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
    # Use risk_adjusted if available, fall back to risk_score
    high_risk = conn.execute("""
        SELECT COUNT(*) FROM anomalies
        WHERE COALESCE(risk_adjusted, risk_score) >= 7
    """).fetchone()[0]
    last_ingest = conn.execute(
        "SELECT MAX(ingested_at) FROM borrows"
    ).fetchone()[0]
    conn.close()

    return {
        "total_borrows":   total_borrows,
        "total_anomalies": total_anomalies,
        "high_risk_count": high_risk,
        "last_ingest":     last_ingest
    }

@app.post("/api/ingest")
def trigger_ingest():
    from dotenv import load_dotenv
    import os
    load_dotenv()
    inserted = run_ingest(os.getenv("GRAPH_API_KEY", ""))
    flagged  = run_detection()
    return {
        "inserted": inserted,
        "flagged":  len(flagged),
        "status":   "ok"
    }

@app.get("/api/config")
def api_get_config():
    return get_config()

@app.post("/api/config")
def api_set_config(data: dict):
    set_config(data)
    # Return the updated config so the client sees what actually stuck
    return {"status": "applied", "config": get_config()}

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=False)

@app.get("/api/wallet/{address}/profile")
def get_wallet_profile(address: str):
    from wallet_intelligence import build_wallet_profile
    from dotenv import load_dotenv
    import os
    load_dotenv()
    return build_wallet_profile(address, os.getenv("GRAPH_API_KEY", ""))
