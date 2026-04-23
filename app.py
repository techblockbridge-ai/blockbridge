# app.py
import sqlite3
import httpx
from pathlib import Path
import os
from datetime import timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for, session
from auth import (
    init_auth_tables, login_required, get_current_user,
    request_magic_link, verify_token, create_session, destroy_session
)

app = Flask(__name__, template_folder="templates")
app.secret_key = os.environ.get("BB_SECRET_KEY", "bb-dev-secret-change-in-prod")
app.permanent_session_lifetime = timedelta(days=7)
API_BASE = "http://localhost:8000/api"
DB_PATH  = Path(__file__).parent / "data" / "blockbridge.db"

def fetch(endpoint):
    try:
        r = httpx.get(f"{API_BASE}/{endpoint}", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {} if "stats" in endpoint else []

@app.route("/")
def landing():
    stats     = fetch("stats")
    anomalies = fetch("anomalies")
    borrows   = fetch("borrows?limit=30")
    user = get_current_user()
    return render_template("index.html", stats=stats, anomalies=anomalies, borrows=borrows,
                           authenticated=(user is not None))


@app.route("/login")
def login_page():
    if get_current_user():
        return redirect("/")
    return render_template("login.html", sent=False, error=None)

@app.route("/auth/request-link", methods=["POST"])
def auth_request_link():
    email = request.form.get("email", "").strip()
    if not email:
        return jsonify({"ok": False, "message": "Enter an email."})
    success, message = request_magic_link(email)
    return jsonify({"ok": success, "message": message})

@app.route("/auth/verify/<token>")
def auth_verify(token):
    email = verify_token(token)
    if not email:
        return render_template("login.html", sent=False,
                               error="Invalid or expired link. Request a new one.")
    create_session(email)
    return redirect("/")

@app.route("/logout")
def logout():
    destroy_session()
    return redirect("/")

@app.route("/weather")
@login_required
def weather():
    """
    Aave Weather Report — plain-English market conditions at a glance.
    Computes protocol status from the same data the dashboard uses,
    but presents it as interpretation, not raw numbers.
    """
    from datetime import datetime, timedelta
    import math

    conn = sqlite3.connect(DB_PATH)

    # ─── Overall stats ───
    total_borrows   = conn.execute("SELECT COUNT(*) FROM borrows").fetchone()[0]
    total_anomalies = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
    high_risk       = conn.execute(
        "SELECT COUNT(*) FROM anomalies WHERE COALESCE(risk_adjusted, risk_score) >= 7"
    ).fetchone()[0]

    # ─── Recent anomalies (last 24h vs previous 24h for trend) ───
    now_iso = datetime.utcnow().isoformat()
    h24_ago = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    h48_ago = (datetime.utcnow() - timedelta(hours=48)).isoformat()

    recent_24h = conn.execute(
        "SELECT COUNT(*) FROM anomalies WHERE detected_at >= ?", (h24_ago,)
    ).fetchone()[0]
    prev_24h = conn.execute(
        "SELECT COUNT(*) FROM anomalies WHERE detected_at >= ? AND detected_at < ?",
        (h48_ago, h24_ago)
    ).fetchone()[0]

    recent_high = conn.execute(
        "SELECT COUNT(*) FROM anomalies WHERE detected_at >= ? AND COALESCE(risk_adjusted, risk_score) >= 7",
        (h24_ago,)
    ).fetchone()[0]

    # ─── Per-asset weather ───
    asset_rows = conn.execute("""
        SELECT b.asset,
               COUNT(DISTINCT b.id) as borrow_count,
               COUNT(DISTINCT a.id) as anomaly_count,
               MAX(a.zscore) as max_zscore,
               MAX(COALESCE(a.risk_adjusted, a.risk_score)) as max_risk
        FROM borrows b
        LEFT JOIN anomalies a ON a.asset = b.asset
        GROUP BY b.asset
        ORDER BY anomaly_count DESC, borrow_count DESC
    """).fetchall()

    # ─── Top 3 signals (most recent HIGH/MEDIUM anomalies) ───
    signals = conn.execute("""
        SELECT asset, amount_usd, zscore,
               COALESCE(risk_adjusted, risk_score) as risk,
               narrative, detected_at
        FROM anomalies
        WHERE COALESCE(risk_adjusted, risk_score) >= 3
        ORDER BY detected_at DESC
        LIMIT 3
    """).fetchall()

    # ─── Last ingest timestamp ───
    last_ingest = conn.execute("SELECT MAX(ingested_at) FROM borrows").fetchone()[0]

    conn.close()

    # ─── Compute overall status ───
    if recent_high >= 3 or recent_24h >= 10:
        status = "STORM"
        status_headline = "Unusual activity detected on Aave"
        status_detail = (f"{recent_high} high-risk events in the last 24 hours. "
                         "Large or unusual borrows are happening more frequently than normal. "
                         "If you're planning to borrow or have open positions, check your health factor.")
    elif recent_24h >= 3 or high_risk >= 1:
        status = "ELEVATED"
        status_headline = "Some unusual activity on Aave"
        status_detail = (f"{recent_24h} anomalies flagged in the last 24 hours. "
                         "A few borrows are larger than typical. Nothing critical yet, "
                         "but worth monitoring if you have positions in the affected assets.")
    else:
        status = "CLEAR"
        status_headline = "Aave borrowing activity looks normal"
        status_detail = ("No significant anomalies in the last 24 hours. "
                         "Borrowing volumes are within expected ranges across all monitored assets. "
                         "Standard conditions for lending and borrowing.")

    # ─── Trend ───
    if prev_24h == 0 and recent_24h == 0:
        trend = "STABLE"
        trend_text = "No change — quiet period"
    elif recent_24h > prev_24h * 1.5:
        trend = "RISING"
        trend_text = f"Activity increasing — {recent_24h} anomalies vs {prev_24h} in prior 24h"
    elif recent_24h < prev_24h * 0.5 and prev_24h > 0:
        trend = "FALLING"
        trend_text = f"Calming down — {recent_24h} anomalies vs {prev_24h} in prior 24h"
    else:
        trend = "STABLE"
        trend_text = f"Steady — {recent_24h} anomalies in last 24h"

    # ─── Per-asset conditions ───
    assets = []
    for row in asset_rows:
        asset_name, borrow_count, anomaly_count, max_z, max_risk = row
        max_z = max_z or 0
        max_risk = max_risk or 0
        anomaly_rate = anomaly_count / max(borrow_count, 1)

        if max_risk >= 7 or anomaly_rate > 0.15:
            condition = "STORM"
            condition_text = "Unusual activity"
        elif max_risk >= 3 or anomaly_rate > 0.05:
            condition = "ELEVATED"
            condition_text = "Slightly elevated"
        else:
            condition = "CLEAR"
            condition_text = "Normal"

        assets.append({
            "name":           asset_name,
            "borrow_count":   borrow_count,
            "anomaly_count":  anomaly_count,
            "max_zscore":     max_z,
            "max_risk":       max_risk,
            "condition":      condition,
            "condition_text": condition_text,
        })

    # ─── Format signals ───
    formatted_signals = []
    for s in signals:
        asset, amount, zs, risk, narrative, detected = s
        # Time ago
        try:
            det_time = datetime.fromisoformat(detected)
            delta = datetime.utcnow() - det_time
            if delta.total_seconds() < 3600:
                ago = f"{int(delta.total_seconds() / 60)}m ago"
            elif delta.total_seconds() < 86400:
                ago = f"{int(delta.total_seconds() / 3600)}h ago"
            else:
                ago = f"{int(delta.days)}d ago"
        except Exception:
            ago = "recently"

        formatted_signals.append({
            "asset":     asset,
            "amount":    amount,
            "zscore":    zs,
            "risk":      risk,
            "narrative": narrative or f"${amount:,.0f} {asset} borrow flagged at {zs:.1f}σ",
            "ago":       ago,
            "risk_label": "HIGH" if risk >= 7 else "MEDIUM",
        })

    return render_template("weather.html",
        status=status,
        status_headline=status_headline,
        status_detail=status_detail,
        trend=trend,
        trend_text=trend_text,
        assets=assets,
        signals=formatted_signals,
        total_borrows=total_borrows,
        total_anomalies=total_anomalies,
        recent_24h=recent_24h,
        last_ingest=last_ingest,
    )

@app.route("/anomalies-table")
@login_required
def anomalies_table():
    return render_template("_anomalies_interactive.html", anomalies=fetch("anomalies"))

@app.route("/borrows-feed")
@login_required
def borrows_feed():
    return render_template("_borrows_feed.html", borrows=fetch("borrows?limit=30"))

@app.route("/api/stats-fragment")
@login_required
def stats_fragment():
    s = fetch("stats")
    return f'''<div class="stats-grid" id="stats-grid" hx-get="/api/stats-fragment" hx-trigger="every 60s" hx-swap="outerHTML">
  <div class="stat-card" style="--accent-color:var(--purple)">
    <div class="stat-label-row"><span class="stat-label-text">Total borrows</span></div>
    <div class="stat-value" style="color:var(--purple)">{s.get("total_borrows",0)}</div>
    <div class="stat-meta">across all assets</div>
  </div>
  <div class="stat-card" style="--accent-color:var(--amber)">
    <div class="stat-label-row"><span class="stat-label-text">Anomalies flagged</span></div>
    <div class="stat-value" style="color:var(--amber)">{s.get("total_anomalies",0)}</div>
    <div class="stat-meta">z-score threshold active</div>
  </div>
  <div class="stat-card" style="--accent-color:var(--red)">
    <div class="stat-label-row"><span class="stat-label-text">High risk events</span></div>
    <div class="stat-value" style="color:var(--red)">{s.get("high_risk_count",0)}</div>
    <div class="stat-meta">risk score &ge; 7.0</div>
  </div>
  <div class="stat-card" style="--accent-color:var(--green)">
    <div class="stat-label-row"><span class="stat-label-text">Last ingest</span></div>
    <div class="stat-value" style="font-size:0.9rem;padding-top:6px;color:var(--green)">{str(s.get("last_ingest","N/A"))[:19].replace("T"," ")}</div>
    <div class="stat-meta">UTC &middot; 60s poll</div>
  </div>
</div>'''

@app.route("/api/config", methods=["GET"])
@login_required
def config_get():
    try:
        r = httpx.get(f"{API_BASE}/config", timeout=5)
        return jsonify(r.json())
    except Exception:
        return jsonify({"zscore_threshold": 2.0, "rolling_window": 50, "min_borrow_size": 0})

@app.route("/api/config", methods=["POST"])
@login_required
def config():
    data = request.get_json()
    try:
        r = httpx.post(f"{API_BASE}/config", json=data, timeout=5)
        return jsonify(r.json())
    except Exception:
        return jsonify({"status": "error", "message": "Backend unreachable — config not saved"}), 503

@app.route("/api/chat", methods=["POST"])
@login_required
def chat_proxy():
    data = request.get_json()
    try:
        r = httpx.post("http://localhost:11434/api/chat", json=data, timeout=60)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/investigate/<int:anomaly_id>")
@login_required
def investigate(anomaly_id):
    conn = sqlite3.connect(DB_PATH)

    row = conn.execute("""
        SELECT a.id, a.borrow_id,
               COALESCE(a.wallet, b.wallet) as wallet,
               a.tx_hash,
               a.asset, a.amount_usd, a.zscore, a.risk_score,
               a.risk_adjusted, a.risk_flag, a.health_factor,
               a.liquidation_price, a.liq_gap_pct, a.narrative, a.detected_at,
               COALESCE(w.is_contract, -1) as is_contract,
               w.contract_name
        FROM anomalies a
        LEFT JOIN borrows b ON a.borrow_id = b.id
        LEFT JOIN wallets w ON COALESCE(a.wallet, b.wallet) = w.address
        WHERE a.id = ?
    """, (anomaly_id,)).fetchone()

    if not row:
        conn.close()
        return "Investigation not found", 404

    # Extract tx_hash from borrow_id if not stored directly
    # borrow_id format: block:index:0xTXHASH:log:log
    stored_tx_hash = row[3]
    if stored_tx_hash is None:
        parts = row[1].split(":")
        stored_tx_hash = parts[2] if len(parts) >= 3 and parts[2].startswith("0x") else None

    # Wallet context from local DB
    wallet = row[2]
    if wallet:
        total_borrows = conn.execute(
            "SELECT COUNT(*) FROM borrows WHERE wallet = ?", (wallet,)
        ).fetchone()[0]
        prior_anomalies = conn.execute(
            "SELECT COUNT(*) FROM anomalies WHERE wallet = ? AND id != ?",
            (wallet, anomaly_id)
        ).fetchone()[0]
        first_seen = conn.execute(
            "SELECT MIN(ingested_at) FROM borrows WHERE wallet = ?", (wallet,)
        ).fetchone()[0]
        first_seen = first_seen[:10] if first_seen else "N/A"
    else:
        total_borrows   = 0
        prior_anomalies = 0
        first_seen      = "N/A"

    conn.close()

    effective_risk = row[8] if row[8] is not None else row[7]

    anomaly = {
        "id":                row[0],
        "borrow_id":         row[1],
        "wallet":            wallet,
        "tx_hash":           stored_tx_hash,
        "asset":             row[4],
        "amount_usd":        row[5],
        "zscore":            row[6],
        "risk_score":        row[7],
        "risk_adjusted":     row[8],
        "risk_flag":         row[9]   if row[9]  is not None else "UNKNOWN",
        "health_factor":     row[10],
        "liquidation_price": row[11],
        "liq_gap_pct":       row[12],
        "narrative":         row[13]  if row[13] is not None else "",
        "detected_at":       row[14],
        "risk_label":        "HIGH" if effective_risk >= 7 else "MEDIUM" if effective_risk >= 3 else "LOW",
        "is_contract":       row[15],
        "contract_name":     row[16],
        # Wallet context
        "total_borrows":     total_borrows,
        "prior_anomalies":   prior_anomalies,
        "first_seen":        first_seen,
    }

    return render_template("investigation.html", anomaly=anomaly)

@app.route("/api/is-contract")
def is_contract():
    address = request.args.get("address", "")
    if not address:
        return jsonify({"is_contract": False, "error": "missing address"})
    try:
        # Use Blockscout — no API key required
        r = httpx.get(
            f"https://eth.blockscout.com/api/v2/addresses/{address}",
            timeout=8
        )
        data          = r.json()
        is_c          = data.get("is_contract", False)
        name          = data.get("name") or None
        ens           = data.get("ens_domain_name") or None
        display_name  = name or ens or None
        return jsonify({
            "is_contract":   is_c,
            "address":       address,
            "contract_name": display_name,
            "ens":           ens
        })
    except Exception as e:
        return jsonify({"is_contract": False, "error": str(e)})

@app.route("/api/wallet/<address>/profile")
def wallet_profile(address):
    try:
        r = httpx.get(f"http://localhost:8000/api/wallet/{address}/profile", timeout=15)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/favicon.ico")
def favicon():
    return "", 204

init_auth_tables()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
