# =============================================================================
# BlockBridge Auth Integration — Blurred Dashboard Approach
# =============================================================================
#
# Unauthenticated visitors see the REAL live dashboard, blurred and disabled,
# with a floating sign-in card. Authenticated users get the full experience.
#
# FILES:
#   auth.py                      → Auth module (magic links, sessions, allowlist CLI)
#   templates/_auth_overlay.html → Blur overlay + sign-in card (injected when not logged in)
#   templates/login.html         → Standalone login page (for direct /login access)
#
# ─────────────────────────────────────────────────────────────────────────────
# DEPLOYMENT STEPS
# ─────────────────────────────────────────────────────────────────────────────
#
# 1. Copy files into ~/blockbridge/
#      cp auth.py ~/blockbridge/auth.py
#      cp templates/_auth_overlay.html ~/blockbridge/templates/_auth_overlay.html
#      cp templates/login.html ~/blockbridge/templates/login.html
#
# 2. Initialize auth tables
#      cd ~/blockbridge && python3 auth.py list
#
# 3. Add yourself to the allowlist
#      python3 auth.py add your@email.com "admin"
#
# 4. Generate a secret key
#      python3 -c "import secrets; print(secrets.token_hex(32))"
#
# 5. Set environment variables (add to ~/.bashrc or systemd override)
#      export BB_SMTP_USER="youraddress@gmail.com"
#      export BB_SMTP_PASS="your-16-char-app-password"
#      export BB_SECRET_KEY="<output from step 4>"
#      export BB_SITE_URL="https://blockbridge.tech"
#
#    Gmail app password: https://myaccount.google.com/apppasswords
#    → Select "Mail" → "Other" → name it "BlockBridge" → copy the 16-char code
#
# 6. Modify app.py — see changes below
#
# 7. Add the overlay include to templates/index.html — add before </body>:
#
#      {% if not authenticated %}
#      {% include '_auth_overlay.html' %}
#      {% endif %}
#
# 8. Update systemd and restart
#      sudo systemctl edit blockbridge-app
#      # Add under [Service]:
#      #   Environment="BB_SMTP_USER=..."
#      #   Environment="BB_SMTP_PASS=..."
#      #   Environment="BB_SECRET_KEY=..."
#      #   Environment="BB_SITE_URL=https://blockbridge.tech"
#
#      sudo systemctl restart blockbridge-app blockbridge-api
#
# ─────────────────────────────────────────────────────────────────────────────
# app.py — WHAT TO CHANGE
# ─────────────────────────────────────────────────────────────────────────────
#
# A) Add these imports at the top:
#
#   import os
#   from datetime import timedelta
#   from flask import session, redirect, url_for
#   from auth import (
#       init_auth_tables, login_required, get_current_user,
#       request_magic_link, verify_token, create_session, destroy_session
#   )
#
# B) After app = Flask(...), add:
#
#   app.secret_key = os.environ.get("BB_SECRET_KEY", "CHANGE-ME")
#   app.permanent_session_lifetime = timedelta(days=7)
#
# C) Change the index route to pass `authenticated`:
#
#   @app.route("/")
#   def index():
#       stats     = fetch("stats")
#       anomalies = fetch("anomalies")
#       borrows   = fetch("borrows?limit=30")
#       user = get_current_user()
#       return render_template("index.html",
#                              stats=stats, anomalies=anomalies, borrows=borrows,
#                              authenticated=(user is not None))
#
# D) Add the auth routes (anywhere before if __name__):
#
#   @app.route("/login")
#   def login_page():
#       if get_current_user():
#           return redirect("/")
#       return render_template("login.html", sent=False, error=None)
#
#   @app.route("/auth/request-link", methods=["POST"])
#   def auth_request_link():
#       email = request.form.get("email", "").strip()
#       if not email:
#           return jsonify({"ok": False, "message": "Enter an email."})
#       success, message = request_magic_link(email)
#       return jsonify({"ok": True, "message": message})
#
#   @app.route("/auth/verify/<token>")
#   def auth_verify(token):
#       email = verify_token(token)
#       if not email:
#           return render_template("login.html", sent=False,
#                                  error="Invalid or expired link. Request a new one.")
#       create_session(email)
#       return redirect("/")
#
#   @app.route("/logout")
#   def logout():
#       destroy_session()
#       return redirect("/")
#
# E) Add @login_required to all protected routes:
#
#   @app.route("/anomalies-table")
#   @login_required        # ← add this
#   def anomalies_table():
#       ...
#
#   @app.route("/borrows-feed")
#   @login_required        # ← add this
#   def borrows_feed():
#       ...
#
#   Same for: /api/stats-fragment, /api/config, /api/chat,
#             /investigate/<id>, /weather, and any other routes
#
# F) Add init_auth_tables() call before if __name__:
#
#   init_auth_tables()
#
# ─────────────────────────────────────────────────────────────────────────────
# TESTING CHECKLIST
# ─────────────────────────────────────────────────────────────────────────────
#
#   1. Incognito → blockbridge.tech
#      ✓ Dashboard visible but blurred
#      ✓ Nothing is clickable
#      ✓ Sign-in card floating in center
#      ✓ No console errors from killed HTMX polling
#
#   2. Enter allowed email → click send
#      ✓ Card flips to "check your email" state
#      ✓ Email arrives with styled magic link
#
#   3. Click magic link
#      ✓ Redirects to / with blur removed, full access
#
#   4. Enter non-allowed email
#      ✓ Same "check your email" message (no info leak)
#      ✓ No email sent
#
#   5. /logout → blur returns
#
#   6. /investigate/123 while logged out → redirects to /
#
#   7. Expired magic link → shows error on login page
#
# ─────────────────────────────────────────────────────────────────────────────
# ALLOWLIST MANAGEMENT (CLI on imhotep)
# ─────────────────────────────────────────────────────────────────────────────
#
#   cd ~/blockbridge
#   python3 auth.py add friend@example.com "early tester"
#   python3 auth.py remove friend@example.com
#   python3 auth.py list
#
