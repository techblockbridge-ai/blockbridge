# auth.py — BlockBridge Magic Link Authentication
# Drop this file into ~/blockbridge/ alongside app.py

import os
import sqlite3
import secrets
import smtplib
import hashlib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from functools import wraps
from flask import session, redirect, url_for, request

# ---------------------------------------------------------------------------
# Config — set these via environment variables or edit defaults
# ---------------------------------------------------------------------------
DB_PATH          = os.environ.get("BB_DB_PATH", os.path.expanduser("~/blockbridge/data/blockbridge.db"))
SMTP_HOST        = os.environ.get("BB_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT        = int(os.environ.get("BB_SMTP_PORT", "587"))
SMTP_USER        = os.environ.get("BB_SMTP_USER", "")          # your gmail address
SMTP_PASS        = os.environ.get("BB_SMTP_PASS", "")          # gmail app password
FROM_EMAIL       = os.environ.get("BB_FROM_EMAIL", "")          # defaults to SMTP_USER if blank
SITE_URL         = os.environ.get("BB_SITE_URL", "https://blockbridge.tech")
TOKEN_EXPIRY_MIN = int(os.environ.get("BB_TOKEN_EXPIRY_MIN", "15"))
SESSION_DAYS     = int(os.environ.get("BB_SESSION_DAYS", "7"))

if not FROM_EMAIL:
    FROM_EMAIL = SMTP_USER

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_auth_tables():
    """Create auth tables if they don't exist. Safe to call on every startup."""
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS allowed_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            added_at TEXT DEFAULT (datetime('now')),
            note TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS magic_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            token_hash TEXT UNIQUE NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            expires_at TEXT NOT NULL,
            used INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS auth_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT UNIQUE NOT NULL,
            email TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            expires_at TEXT NOT NULL
        );
    """)
    conn.commit()
    conn.close()

# ---------------------------------------------------------------------------
# Allowlist management (CLI helpers)
# ---------------------------------------------------------------------------
def allow_email(email, note=""):
    """Add an email to the allowlist."""
    conn = get_db()
    try:
        conn.execute("INSERT OR IGNORE INTO allowed_emails (email, note) VALUES (?, ?)",
                      (email.lower().strip(), note))
        conn.commit()
        print(f"✓ Allowed: {email.lower().strip()}")
    finally:
        conn.close()

def remove_email(email):
    """Remove an email from the allowlist."""
    conn = get_db()
    conn.execute("DELETE FROM allowed_emails WHERE email = ?", (email.lower().strip(),))
    conn.commit()
    conn.close()
    print(f"✗ Removed: {email.lower().strip()}")

def list_allowed():
    """Print all allowed emails."""
    conn = get_db()
    rows = conn.execute("SELECT email, added_at, note FROM allowed_emails ORDER BY added_at").fetchall()
    conn.close()
    if not rows:
        print("No allowed emails.")
        return
    for r in rows:
        note = f"  ({r['note']})" if r['note'] else ""
        print(f"  {r['email']}  — added {r['added_at']}{note}")

def is_allowed(email):
    conn = get_db()
    row = conn.execute("SELECT 1 FROM allowed_emails WHERE email = ?",
                        (email.lower().strip(),)).fetchone()
    conn.close()
    return row is not None

# ---------------------------------------------------------------------------
# Magic link generation & sending
# ---------------------------------------------------------------------------
def _hash_token(token):
    return hashlib.sha256(token.encode()).hexdigest()

def create_magic_link(email):
    """Generate a magic link token, store its hash, return the full URL."""
    email = email.lower().strip()
    token = secrets.token_urlsafe(48)
    token_hash = _hash_token(token)
    expires = datetime.now(timezone.utc) + timedelta(minutes=TOKEN_EXPIRY_MIN)

    conn = get_db()
    # Clean up old tokens for this email
    conn.execute("DELETE FROM magic_tokens WHERE email = ? OR expires_at < datetime('now')", (email,))
    conn.execute(
        "INSERT INTO magic_tokens (email, token_hash, expires_at) VALUES (?, ?, ?)",
        (email, token_hash, expires.strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn.commit()
    conn.close()

    return f"{SITE_URL}/auth/verify/{token}"

def send_magic_email(email, link):
    """Send the magic link email via Gmail SMTP."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Your BlockBridge access link"
    msg["From"] = f"BlockBridge <{FROM_EMAIL}>"
    msg["To"] = email

    text_body = f"""BlockBridge — Aave V3 Intelligence

Your sign-in link (expires in {TOKEN_EXPIRY_MIN} minutes):

{link}

If you didn't request this, ignore this email.
"""

    html_body = f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#0a0a0f;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
<div style="max-width:480px;margin:40px auto;padding:40px;background:#12121a;border:1px solid rgba(0,212,255,0.15);border-radius:12px;">
  <div style="text-align:center;margin-bottom:32px;">
    <span style="display:inline-flex;align-items:center;justify-content:center;width:48px;height:48px;background:rgba(0,212,255,0.1);border:1px solid rgba(0,212,255,0.3);border-radius:10px;font-weight:700;color:#00d4ff;font-size:18px;">BB</span>
    <div style="color:#fff;font-size:18px;font-weight:600;margin-top:12px;">BlockBridge</div>
    <div style="color:#666;font-size:12px;letter-spacing:0.5px;margin-top:4px;">AAVE V3 INTELLIGENCE</div>
  </div>

  <p style="color:#ccc;font-size:14px;line-height:1.6;margin:0 0 24px;">
    Click below to sign in. This link expires in <strong style="color:#fff;">{TOKEN_EXPIRY_MIN} minutes</strong>.
  </p>

  <div style="text-align:center;margin:32px 0;">
    <a href="{link}" style="display:inline-block;padding:14px 36px;background:rgba(0,212,255,0.12);border:1px solid rgba(0,212,255,0.4);border-radius:8px;color:#00d4ff;text-decoration:none;font-size:14px;font-weight:600;letter-spacing:0.3px;">
      Sign in to BlockBridge →
    </a>
  </div>

  <p style="color:#555;font-size:11px;line-height:1.5;margin:24px 0 0;border-top:1px solid rgba(255,255,255,0.06);padding-top:16px;">
    If you didn't request this link, you can safely ignore this email.<br>
    Link: <span style="color:#444;word-break:break-all;">{link}</span>
  </p>
</div>
</body>
</html>"""

    msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(FROM_EMAIL, email, msg.as_string())

def request_magic_link(email):
    """Full flow: validate allowlist → create token → send email. Returns (success, message)."""
    email = email.lower().strip()
    if not is_allowed(email):
        # Don't reveal whether the email exists — always show same message
        return True, "If that email is on our access list, you'll receive a sign-in link shortly."

    link = create_magic_link(email)
    try:
        send_magic_email(email, link)
    except Exception as e:
        print(f"[AUTH] Email send failed for {email}: {e}")
        return False, "Something went wrong sending your link. Please try again."

    return True, "If that email is on our access list, you'll receive a sign-in link shortly."

# ---------------------------------------------------------------------------
# Token verification & session creation
# ---------------------------------------------------------------------------
def verify_token(token):
    """Verify a magic link token. Returns email on success, None on failure."""
    token_hash = _hash_token(token)
    conn = get_db()
    row = conn.execute(
        "SELECT email, expires_at, used FROM magic_tokens WHERE token_hash = ?",
        (token_hash,)
    ).fetchone()

    if not row:
        conn.close()
        return None

    if row["used"]:
        conn.close()
        return None

    if datetime.strptime(row["expires_at"], "%Y-%m-%d %H:%M:%S") < datetime.utcnow():
        conn.close()
        return None

    # Mark as used
    conn.execute("UPDATE magic_tokens SET used = 1 WHERE token_hash = ?", (token_hash,))
    conn.commit()
    conn.close()

    return row["email"]

def create_session(email):
    """Create a new auth session, store it, and set the Flask session cookie."""
    session_id = secrets.token_urlsafe(48)
    expires = datetime.now(timezone.utc) + timedelta(days=SESSION_DAYS)

    conn = get_db()
    conn.execute(
        "INSERT INTO auth_sessions (session_id, email, expires_at) VALUES (?, ?, ?)",
        (session_id, email, expires.strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn.commit()
    conn.close()

    session["bb_session_id"] = session_id
    session["bb_email"] = email
    session.permanent = True

def get_current_user():
    """Check if the current request has a valid session. Returns email or None."""
    session_id = session.get("bb_session_id")
    if not session_id:
        return None

    conn = get_db()
    row = conn.execute(
        "SELECT email, expires_at FROM auth_sessions WHERE session_id = ?",
        (session_id,)
    ).fetchone()
    conn.close()

    if not row:
        return None

    if datetime.strptime(row["expires_at"], "%Y-%m-%d %H:%M:%S") < datetime.utcnow():
        destroy_session()
        return None

    return row["email"]

def destroy_session():
    """Log out — remove session from DB and clear cookie."""
    session_id = session.get("bb_session_id")
    if session_id:
        conn = get_db()
        conn.execute("DELETE FROM auth_sessions WHERE session_id = ?", (session_id,))
        conn.commit()
        conn.close()
    session.clear()

# ---------------------------------------------------------------------------
# Flask decorator
# ---------------------------------------------------------------------------
def login_required(f):
    """Decorator: redirect to landing page if not authenticated."""
    @wraps(f)
    def decorated(*args, **kwargs):
        user = get_current_user()
        if not user:
            return redirect(url_for("landing"))
        return f(*args, **kwargs)
    return decorated

# ---------------------------------------------------------------------------
# CLI management tool
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    init_auth_tables()

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python auth.py add <email> [note]    — Add email to allowlist")
        print("  python auth.py remove <email>         — Remove email")
        print("  python auth.py list                   — List all allowed emails")
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd == "add" and len(sys.argv) >= 3:
        note = " ".join(sys.argv[3:]) if len(sys.argv) > 3 else ""
        allow_email(sys.argv[2], note)
    elif cmd == "remove" and len(sys.argv) >= 3:
        remove_email(sys.argv[2])
    elif cmd == "list":
        list_allowed()
    else:
        print("Unknown command. Use add/remove/list.")
