"""Unified data sync: Google Sheets + ERPNext → SQLite.

Background thread syncs all sources at configurable intervals.
All reads go through SQLite — never direct to Google Sheets in the request path.
"""

import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta

import requests

# ---- Config ----

DB_PATH = os.path.join(os.path.dirname(__file__), "history.db")

BOARD_JOB_CARD_ID = "1ksKGJRzBvyzgwhXu5ZD9QfM1f1Hfq9MA8Ons-ris99g"
DAILY_PLAN_ID = "1ijtNeYhrEER6G8QlJs9ErL7TPmCc-SiiaGQpdOQzsPE"
MPV3_ID = "1orR4_YhWN-jEvAHZRn2e_crIhRWbemE9BSEvzPJlQYI"
SOV3_ID = "1mo8yEcY7V6lMMpDpjHv6MTxfwLXvsQCkgvJk1MXfrhM"

# Sync intervals in seconds
BOARD_SYNC_INTERVAL = 1800   # 30 min for board job card
ENRICH_SYNC_INTERVAL = 3000  # 50 min for FG, routing, MPV3, SOV3

# In-memory caches
_caches = {}
_cache_lock = threading.Lock()


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_tables():
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS fg_stock (
            item_code TEXT PRIMARY KEY,
            actual_qty REAL,
            location TEXT
        );
        CREATE TABLE IF NOT EXISTS first_machine (
            item_code TEXT PRIMARY KEY,
            machine TEXT
        );
        CREATE TABLE IF NOT EXISTS monthly_plan (
            item_code TEXT PRIMARY KEY,
            customer TEXT,
            pending_qty REAL,
            final_pending_qty REAL,
            fg_qty REAL,
            pending_monthly_plan REAL
        );
        CREATE TABLE IF NOT EXISTS so_pending (
            item_code TEXT PRIMARY KEY,
            customer TEXT,
            total_qty REAL,
            invoiced_qty REAL,
            pending_qty REAL,
            so_count INTEGER
        );
    """)
    conn.commit()
    conn.close()


_init_tables()


# ---- Google Sheets reader (reuse credentials from sheets module) ----

def _read_sheet(sheet_name, spreadsheet_id):
    """Read sheet via Google Sheets API."""
    from sheets import _get_service
    service = _get_service()
    data = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'"
    ).execute()
    return data.get("values", [])


def _col(row, idx):
    return row[idx].strip() if len(row) > idx and row[idx] else ""


# ---- Sync: FG Stock (ALL Point FG) ----

def sync_fg_stock():
    print("[sync] FG stock...")
    rows = _read_sheet("ALL Point FG", DAILY_PLAN_ID)
    if not rows or len(rows) < 2:
        return

    summary = {}
    for row in rows[1:]:
        erp_code = _col(row, 3)
        qty_str = _col(row, 5)
        location = _col(row, 6)
        if not erp_code:
            continue
        try:
            qty = float(qty_str) if qty_str else 0
        except (ValueError, TypeError):
            qty = 0

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
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('fg_last_sync', ?)",
                 (datetime.now().isoformat(),))
    conn.commit()
    conn.close()
    print(f"[sync] FG: {len(summary)} items")


# ---- Sync: First Machine Routing ----

def sync_first_machine():
    print("[sync] First Machine Routing...")
    rows = _read_sheet("First Machine Routing", DAILY_PLAN_ID)
    if not rows or len(rows) < 2:
        return

    conn = _get_conn()
    conn.execute("DELETE FROM first_machine")
    count = 0
    for row in rows[1:]:
        item_name = _col(row, 1)
        machine = _col(row, 2)
        if item_name and machine:
            conn.execute(
                "INSERT OR REPLACE INTO first_machine (item_code, machine) VALUES (?, ?)",
                (item_name, machine)
            )
            count += 1
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('fm_last_sync', ?)",
                 (datetime.now().isoformat(),))
    conn.commit()
    conn.close()
    print(f"[sync] First Machine: {count} items")


# ---- Sync: MPV3 (Monthly Plan) ----

def sync_monthly_plan():
    print("[sync] Monthly Plan (MPV3)...")
    rows = _read_sheet("Auto Working Sheet", MPV3_ID)
    if not rows or len(rows) < 3:
        return

    # Key columns: [1] Customer, [24] Item Code, [33] Pending Qty,
    # [35] Final Pending Qty, [36] FG, [56] Pending Monthly Plan
    summary = {}
    for row in rows[2:]:  # skip header rows
        item_code = _col(row, 24)
        if not item_code:
            continue
        customer = _col(row, 1)

        def to_float(idx):
            v = _col(row, idx)
            try:
                return float(v) if v else 0
            except (ValueError, TypeError):
                return 0

        pending = to_float(33)
        final_pending = to_float(35)
        fg = to_float(36)
        pending_mp = to_float(56)

        if item_code in summary:
            # Aggregate across multiple rows for same item
            summary[item_code]["pending_qty"] += pending
            summary[item_code]["final_pending_qty"] += final_pending
            summary[item_code]["fg_qty"] += fg
            summary[item_code]["pending_monthly_plan"] += pending_mp
        else:
            summary[item_code] = {
                "customer": customer,
                "pending_qty": pending,
                "final_pending_qty": final_pending,
                "fg_qty": fg,
                "pending_monthly_plan": pending_mp,
            }

    conn = _get_conn()
    conn.execute("DELETE FROM monthly_plan")
    count = 0
    for code, data in summary.items():
        if data["pending_monthly_plan"] > 0 or data["pending_qty"] > 0:
            conn.execute(
                """INSERT INTO monthly_plan
                   (item_code, customer, pending_qty, final_pending_qty, fg_qty, pending_monthly_plan)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (code, data["customer"], data["pending_qty"],
                 data["final_pending_qty"], data["fg_qty"], data["pending_monthly_plan"])
            )
            count += 1
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('mpv3_last_sync', ?)",
                 (datetime.now().isoformat(),))
    conn.commit()
    conn.close()
    print(f"[sync] MPV3: {count} items with pending quantities")


# ---- Sync: SOV3 (Sales Order Pending) ----

def sync_so_pending():
    print("[sync] SO Pending (SOV3)...")
    rows = _read_sheet("Pivot Table 1", SOV3_ID)
    if not rows or len(rows) < 3:
        return

    # [0] Customer Name, [2] Item Code, [9] SUM of QTY,
    # [10] SUM of Invoiced Qty, [11] SUM of Final Pending Qty
    summary = {}
    for row in rows[2:]:  # skip header + blank row
        item_code = _col(row, 2)
        if not item_code:
            continue
        customer = _col(row, 0)

        def to_float(idx):
            v = _col(row, idx)
            try:
                return float(v) if v else 0
            except (ValueError, TypeError):
                return 0

        total_qty = to_float(9)
        invoiced = to_float(10)
        pending = to_float(11)

        if pending <= 0:
            continue

        if item_code in summary:
            summary[item_code]["total_qty"] += total_qty
            summary[item_code]["invoiced_qty"] += invoiced
            summary[item_code]["pending_qty"] += pending
            summary[item_code]["so_count"] += 1
        else:
            summary[item_code] = {
                "customer": customer,
                "total_qty": total_qty,
                "invoiced_qty": invoiced,
                "pending_qty": pending,
                "so_count": 1,
            }

    conn = _get_conn()
    conn.execute("DELETE FROM so_pending")
    for code, data in summary.items():
        conn.execute(
            """INSERT INTO so_pending
               (item_code, customer, total_qty, invoiced_qty, pending_qty, so_count)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (code, data["customer"], data["total_qty"],
             data["invoiced_qty"], data["pending_qty"], data["so_count"])
        )
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('sov3_last_sync', ?)",
                 (datetime.now().isoformat(),))
    conn.commit()
    conn.close()
    print(f"[sync] SOV3: {len(summary)} items with pending SOs")


# ---- Read helpers (from SQLite, used by sheets.py) ----

def get_fg_stock():
    """item_code -> qty"""
    with _cache_lock:
        if "fg" in _caches and (time.time() - _caches["fg"]["ts"]) < 300:
            return _caches["fg"]["data"]

    conn = _get_conn()
    rows = conn.execute("SELECT item_code, actual_qty FROM fg_stock").fetchall()
    conn.close()
    result = {r["item_code"]: r["actual_qty"] for r in rows}
    with _cache_lock:
        _caches["fg"] = {"data": result, "ts": time.time()}
    return result


def get_first_machine():
    """item_code -> machine name"""
    with _cache_lock:
        if "fm" in _caches and (time.time() - _caches["fm"]["ts"]) < 300:
            return _caches["fm"]["data"]

    conn = _get_conn()
    rows = conn.execute("SELECT item_code, machine FROM first_machine").fetchall()
    conn.close()
    result = {r["item_code"]: r["machine"] for r in rows}
    with _cache_lock:
        _caches["fm"] = {"data": result, "ts": time.time()}
    return result


def get_monthly_plan():
    """item_code -> {pending_qty, final_pending_qty, fg_qty, pending_monthly_plan}"""
    with _cache_lock:
        if "mpv3" in _caches and (time.time() - _caches["mpv3"]["ts"]) < 300:
            return _caches["mpv3"]["data"]

    conn = _get_conn()
    rows = conn.execute("SELECT * FROM monthly_plan").fetchall()
    conn.close()
    result = {}
    for r in rows:
        result[r["item_code"]] = {
            "pending_qty": r["pending_qty"],
            "final_pending_qty": r["final_pending_qty"],
            "fg_qty": r["fg_qty"],
            "pending_monthly_plan": r["pending_monthly_plan"],
        }
    with _cache_lock:
        _caches["mpv3"] = {"data": result, "ts": time.time()}
    return result


def get_so_pending():
    """item_code -> {pending_qty, so_count}"""
    with _cache_lock:
        if "sov3" in _caches and (time.time() - _caches["sov3"]["ts"]) < 300:
            return _caches["sov3"]["data"]

    conn = _get_conn()
    rows = conn.execute("SELECT item_code, pending_qty, so_count FROM so_pending").fetchall()
    conn.close()
    result = {}
    for r in rows:
        result[r["item_code"]] = {
            "pending_qty": r["pending_qty"],
            "so_count": r["so_count"],
        }
    with _cache_lock:
        _caches["sov3"] = {"data": result, "ts": time.time()}
    return result


def force_resync_all():
    """Clear all sync timestamps to force fresh sync."""
    with _cache_lock:
        _caches.clear()
    conn = _get_conn()
    conn.execute("DELETE FROM meta WHERE key IN ('fg_last_sync','fm_last_sync','mpv3_last_sync','sov3_last_sync')")
    conn.commit()
    conn.close()


# ---- Background sync thread ----

def _last_sync_age(key):
    """Return seconds since last sync, or infinity if never synced."""
    conn = _get_conn()
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    conn.close()
    if not row:
        return float("inf")
    last = datetime.fromisoformat(row["value"])
    return (datetime.now() - last).total_seconds()


def _sync_loop():
    """Background loop that syncs all data sources at their intervals."""
    print("[sync] Background sync thread started")

    while True:
        try:
            # Enrichment sources (50 min interval)
            if _last_sync_age("fg_last_sync") > ENRICH_SYNC_INTERVAL:
                sync_fg_stock()
            if _last_sync_age("fm_last_sync") > ENRICH_SYNC_INTERVAL:
                sync_first_machine()
            if _last_sync_age("mpv3_last_sync") > ENRICH_SYNC_INTERVAL:
                sync_monthly_plan()
            if _last_sync_age("sov3_last_sync") > ENRICH_SYNC_INTERVAL:
                sync_so_pending()

            # Clear in-memory caches after sync
            with _cache_lock:
                _caches.clear()

        except Exception as e:
            print(f"[sync] Error: {e}")

        time.sleep(60)  # Check every 60 seconds


_sync_thread = None


def start_background_sync():
    """Start the background sync thread (call once from app startup)."""
    global _sync_thread
    if _sync_thread and _sync_thread.is_alive():
        return
    _sync_thread = threading.Thread(target=_sync_loop, daemon=True, name="data-sync")
    _sync_thread.start()
