"""ERPNext API client for Sales Order pending quantities.

Fetches pending SO items and caches in SQLite (daily sync).
"""

import os
import sqlite3
import time
from datetime import datetime, timedelta

import requests

ERP_URL = os.environ.get("ERP_URL", "https://xpertpack.frappe.cloud")
ERP_API_KEY = os.environ.get("ERP_API_KEY", "ec03b51ba4d00b7")
ERP_API_SECRET = os.environ.get("ERP_API_SECRET", "f85442392be701c")

DB_PATH = os.path.join(os.path.dirname(__file__), "history.db")

_so_cache = {}  # in-memory cache: item_code -> {pending_qty, so_count}
_so_cache_ts = 0


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_so_tables():
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS so_summary (
            item_code TEXT PRIMARY KEY,
            total_pending_qty REAL,
            so_count INTEGER
        );
        CREATE TABLE IF NOT EXISTS fg_stock (
            item_code TEXT PRIMARY KEY,
            actual_qty REAL,
            location TEXT
        );
    """)
    conn.commit()
    conn.close()


def _needs_so_sync():
    conn = _get_conn()
    row = conn.execute("SELECT value FROM meta WHERE key='so_last_sync'").fetchone()
    conn.close()
    if row:
        last = datetime.fromisoformat(row["value"])
        return (datetime.now() - last) > timedelta(hours=24)
    return True


def _fetch_pending_so_items():
    """Fetch all pending SO items from ERPNext in bulk."""
    headers = {"Authorization": f"token {ERP_API_KEY}:{ERP_API_SECRET}"}
    all_items = []
    page_start = 0
    page_size = 500

    while True:
        resp = requests.get(
            f"{ERP_URL}/api/resource/Sales Order",
            headers=headers,
            params={
                "filters": '[["per_delivered","<",100],["docstatus","=",1],["status","!=","Closed"]]',
                "fields": '["name","customer","`tabSales Order Item`.item_code","`tabSales Order Item`.qty","`tabSales Order Item`.delivered_qty"]',
                "limit_page_length": page_size,
                "limit_start": page_start,
            },
            timeout=60,
        )
        if resp.status_code != 200:
            print(f"[erpnext] API error {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json().get("data", [])
        if not data:
            break

        all_items.extend(data)
        if len(data) < page_size:
            break
        page_start += page_size

    return all_items


def _sync_so_data():
    """Fetch SO items from ERPNext and save summary to SQLite."""
    print("[erpnext] Syncing pending SO data...")
    items = _fetch_pending_so_items()
    print(f"[erpnext] Fetched {len(items)} SO line items")

    # Aggregate by item_code
    summary = {}  # item_code -> {pending_qty, so_names}
    for item in items:
        code = item.get("item_code", "")
        if not code:
            continue
        qty = (item.get("qty") or 0) - (item.get("delivered_qty") or 0)
        if qty <= 0:
            continue

        if code not in summary:
            summary[code] = {"pending_qty": 0, "so_names": set()}
        summary[code]["pending_qty"] += qty
        summary[code]["so_names"].add(item.get("name", ""))

    # Save to SQLite
    conn = _get_conn()
    conn.execute("DELETE FROM so_summary")
    for code, data in summary.items():
        conn.execute(
            "INSERT INTO so_summary (item_code, total_pending_qty, so_count) VALUES (?, ?, ?)",
            (code, data["pending_qty"], len(data["so_names"]))
        )
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('so_last_sync', ?)",
        (datetime.now().isoformat(),)
    )
    conn.commit()
    conn.close()
    print(f"[erpnext] Saved {len(summary)} item summaries")


def get_so_summary():
    """Get SO pending qty lookup. Returns dict: item_code -> {pending_qty, so_count}."""
    global _so_cache, _so_cache_ts

    # In-memory cache (5 min)
    if _so_cache and (time.time() - _so_cache_ts) < 300:
        return _so_cache

    # Sync from ERPNext if needed
    if _needs_so_sync():
        _sync_so_data()

    # Load from SQLite
    conn = _get_conn()
    rows = conn.execute("SELECT item_code, total_pending_qty, so_count FROM so_summary").fetchall()
    conn.close()

    result = {}
    for row in rows:
        result[row["item_code"]] = {
            "pending_qty": row["total_pending_qty"],
            "so_count": row["so_count"],
        }

    _so_cache = result
    _so_cache_ts = time.time()
    return result


_fg_cache = {}
_fg_cache_ts = 0

DAILY_PLAN_SHEET_ID = "1ijtNeYhrEER6G8QlJs9ErL7TPmCc-SiiaGQpdOQzsPE"
FG_SHEET_NAME = "ALL Point FG"


def _needs_fg_sync():
    conn = _get_conn()
    row = conn.execute("SELECT value FROM meta WHERE key='fg_last_sync'").fetchone()
    conn.close()
    if row:
        last = datetime.fromisoformat(row["value"])
        return (datetime.now() - last) > timedelta(hours=24)
    return True


def _sync_fg_stock():
    """Fetch FG stock from Google Sheet and save to SQLite."""
    from sheets import _read_sheet
    print("[fg] Syncing FG stock from Google Sheet...")
    rows = _read_sheet(FG_SHEET_NAME, spreadsheet_id=DAILY_PLAN_SHEET_ID)
    if not rows or len(rows) < 2:
        print("[fg] No FG data found")
        return

    # Aggregate by Erp Code (col 3), sum QTY (col 5)
    summary = {}  # erp_code -> {qty, location}
    for row in rows[1:]:
        erp_code = row[3].strip() if len(row) > 3 and row[3] else ""
        qty_str = row[5].strip() if len(row) > 5 and row[5] else ""
        location = row[6].strip() if len(row) > 6 and row[6] else ""
        if not erp_code:
            continue
        try:
            qty = float(qty_str) if qty_str else 0
        except (ValueError, TypeError):
            qty = 0
        if qty <= 0:
            continue

        if erp_code in summary:
            summary[erp_code]["qty"] += qty
        else:
            summary[erp_code] = {"qty": qty, "location": location}

    conn = _get_conn()
    conn.execute("DELETE FROM fg_stock")
    for code, data in summary.items():
        conn.execute(
            "INSERT INTO fg_stock (item_code, actual_qty, location) VALUES (?, ?, ?)",
            (code, data["qty"], data["location"])
        )
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('fg_last_sync', ?)",
        (datetime.now().isoformat(),)
    )
    conn.commit()
    conn.close()
    print(f"[fg] Saved FG stock for {len(summary)} items")


def get_fg_stock():
    """Get FG stock lookup. Returns dict: item_code -> actual_qty."""
    global _fg_cache, _fg_cache_ts

    if _fg_cache and (time.time() - _fg_cache_ts) < 300:
        return _fg_cache

    if _needs_fg_sync():
        _sync_fg_stock()

    conn = _get_conn()
    rows = conn.execute("SELECT item_code, actual_qty FROM fg_stock").fetchall()
    conn.close()

    result = {row["item_code"]: row["actual_qty"] for row in rows}
    _fg_cache = result
    _fg_cache_ts = time.time()
    return result


def force_resync():
    """Force re-sync on next access."""
    global _so_cache, _so_cache_ts, _fg_cache, _fg_cache_ts
    _so_cache = {}
    _so_cache_ts = 0
    _fg_cache = {}
    _fg_cache_ts = 0
    conn = _get_conn()
    conn.execute("DELETE FROM meta WHERE key='so_last_sync'")
    conn.execute("DELETE FROM meta WHERE key='fg_last_sync'")
    conn.commit()
    conn.close()


# Initialize tables on import
init_so_tables()
