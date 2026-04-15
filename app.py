import os
import base64
import secrets
from datetime import timedelta
from email.mime.text import MIMEText
from functools import wraps

import requests as http_requests
from flask import (
    Flask, request, redirect, url_for, session,
    render_template, jsonify, flash
)
from authlib.integrations.flask_client import OAuth

import sheets


# ---- Prefix middleware (app lives at /board-planner/ behind nginx) ----

class PrefixMiddleware:
    def __init__(self, app, prefix=""):
        self.app = app
        self.prefix = prefix

    def __call__(self, environ, start_response):
        environ["SCRIPT_NAME"] = self.prefix
        path = environ.get("PATH_INFO", "")
        if path.startswith(self.prefix):
            environ["PATH_INFO"] = path[len(self.prefix):]
        return self.app(environ, start_response)


# ---- Config ----

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.permanent_session_lifetime = timedelta(days=30)
app.wsgi_app = PrefixMiddleware(
    app.wsgi_app,
    prefix=os.environ.get("APP_PREFIX", "/board-planner")
)

ALLOWED_DOMAIN = "xpertpack.in"

# ---- OAuth ----

oauth = OAuth(app)
google = oauth.register(
    name="google",
    client_id=os.environ.get("GOOGLE_CLIENT_ID", ""),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET", ""),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile https://www.googleapis.com/auth/gmail.send"},
)


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ---- Auth Routes ----

@app.route("/login")
def login():
    if session.get("user"):
        return redirect(url_for("planner_page"))
    return render_template("login.html")


@app.route("/auth/login")
def auth_login():
    redirect_uri = url_for("auth_callback", _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route("/auth/callback")
def auth_callback():
    token = google.authorize_access_token()
    user_info = token.get("userinfo") or google.userinfo()
    email = user_info.get("email", "")

    if not email.lower().endswith(f"@{ALLOWED_DOMAIN}"):
        flash(f"Access denied for {email}. Only @{ALLOWED_DOMAIN} emails allowed.", "error")
        return redirect(url_for("login"))

    session.permanent = True
    session["user"] = {"email": email, "name": user_info.get("name", email)}
    session["google_token"] = token.get("access_token", "")
    return redirect(url_for("planner_page"))


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---- App Routes ----

@app.route("/")
@login_required
def index():
    return redirect(url_for("planner_page"))


@app.route("/planner")
@login_required
def planner_page():
    return render_template("planner.html")


# ---- API Routes ----

@app.route("/api/deckle-jobs")
@login_required
def api_deckle_jobs():
    try:
        data = sheets.get_deckle_jobs()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/client-jobs")
@login_required
def api_client_jobs():
    try:
        data = sheets.get_client_jobs()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/deckle-detail")
@login_required
def api_deckle_detail():
    deckle = request.args.get("deckle", "")
    ref_bpro = request.args.get("ref_bpro", "")
    if not deckle:
        return jsonify({"error": "deckle parameter required"}), 400
    try:
        data = sheets.get_deckle_detail(deckle, ref_bpro or None)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/detail")
@login_required
def detail_page():
    deckle = request.args.get("deckle", "")
    ref_bpro = request.args.get("ref_bpro", "")
    return render_template("detail.html", deckle=deckle, ref_bpro=ref_bpro)


@app.route("/api/history")
@login_required
def api_history():
    try:
        data = sheets.get_history_list()
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/refresh")
@login_required
def api_refresh():
    sheets.clear_cache()
    return jsonify({"ok": True})


@app.route("/api/export-plan", methods=["POST"])
@login_required
def api_export_plan():
    body = request.get_json(force=True)
    bpro_list = body.get("jobs", [])
    plan_date = body.get("date", "")

    if not bpro_list:
        return jsonify({"error": "No jobs selected"}), 400

    jobs = sheets.get_jobs_for_export(bpro_list)

    # Build CSV — 9 columns matching daily plan format
    header = "Deckle,BPRO to run,Item (Boards)To Manufacture,IPRO,ITEM CODE,CUSTOMER,Running Name,Item Qty To Mfg.,Remark"
    lines = [header]
    for job in jobs:
        row = [
            job["deckle"],
            job["bpro"],
            job["board_item"],
            job["ipro"],
            job["item_name"],
            job["customer"],
            job["running_name"],
            job["qty"],
            plan_date,
        ]
        # Escape commas in fields
        escaped = []
        for val in row:
            val = str(val)
            if "," in val or '"' in val:
                val = '"' + val.replace('"', '""') + '"'
            escaped.append(val)
        lines.append(",".join(escaped))

    csv_text = "\n".join(lines)
    return jsonify({"csv": csv_text, "job_count": len(jobs), "date": plan_date})


WHATSAPP_GROUPS = [
    "120363425793020306@g.us",
    "120363419163916516@g.us",
]

WASENDER_BASE = "https://api.wasenderapi.com"
WASENDER_API_KEY = os.environ.get("WASENDER_API_KEY", "")


@app.route("/api/send-bpro-request", methods=["POST"])
@login_required
def api_send_bpro_request():
    body = request.get_json(force=True)
    message = body.get("message", "")

    if not message:
        return jsonify({"error": "No message to send"}), 400

    if not WASENDER_API_KEY:
        return jsonify({"error": "WhatsApp API key not configured"}), 500

    results = []
    for group_jid in WHATSAPP_GROUPS:
        try:
            resp = http_requests.post(
                f"{WASENDER_BASE}/api/send-message",
                json={"to": group_jid, "text": message},
                headers={
                    "Authorization": f"Bearer {WASENDER_API_KEY}",
                    "Content-Type": "application/json",
                },
                timeout=30,
            )
            results.append({"group": group_jid, "status": resp.status_code, "ok": 200 <= resp.status_code < 300})
        except Exception as e:
            results.append({"group": group_jid, "status": 0, "ok": False, "error": str(e)})

    success_count = sum(1 for r in results if r["ok"])
    return jsonify({
        "ok": success_count > 0,
        "message": f"Sent to {success_count}/{len(WHATSAPP_GROUPS)} groups",
        "results": results,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=True)
