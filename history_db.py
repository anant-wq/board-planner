"""SQLite cache for 90-day production history.

Fetches from Google Sheets once per day, stores locally in SQLite.
Subsequent reads are instant from the local DB.
"""

import os
import sqlite3
import json
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "history.db")


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = _get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS history (
            bpro TEXT,
            deckle TEXT,
            deckle_raw TEXT,
            item_code TEXT,
            runs INTEGER,
            total_qty INTEGER,
            last_run TEXT,
            first_run TEXT,
            PRIMARY KEY (bpro, deckle)
        );
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS pivot_cache (
            key TEXT PRIMARY KEY,
            data TEXT,
            updated_at TEXT
        );
    """)
    conn.commit()
    conn.close()


def _get_last_sync():
    conn = _get_conn()
    row = conn.execute("SELECT value FROM meta WHERE key='last_sync'").fetchone()
    conn.close()
    if row:
        return datetime.fromisoformat(row["value"])
    return None


def _set_last_sync():
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('last_sync', ?)",
        (datetime.now().isoformat(),)
    )
    conn.commit()
    conn.close()


def needs_sync():
    """Check if we need to re-fetch from Google Sheets (once per day)."""
    last = _get_last_sync()
    if not last:
        return True
    return (datetime.now() - last) > timedelta(hours=24)


def save_history(parsed_history):
    """Save parsed history dict (deckle -> {bpro -> entry}) to SQLite."""
    conn = _get_conn()
    conn.execute("DELETE FROM history")
    for deckle, bpros in parsed_history.items():
        for bpro, entry in bpros.items():
            conn.execute(
                """INSERT OR REPLACE INTO history
                   (bpro, deckle, deckle_raw, item_code, runs, total_qty, last_run, first_run)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    entry["bpro"],
                    deckle,
                    entry["deckle_raw"],
                    entry.get("item_code", ""),
                    entry["runs"],
                    entry["total_qty"],
                    entry["last_run"].isoformat() if isinstance(entry["last_run"], datetime) else entry["last_run"],
                    entry["first_run"].isoformat() if isinstance(entry["first_run"], datetime) else entry["first_run"],
                )
            )
    conn.commit()
    _set_last_sync()
    conn.close()


def load_history():
    """Load history from SQLite, returns same format as _parse_history()."""
    conn = _get_conn()
    rows = conn.execute("SELECT * FROM history").fetchall()
    conn.close()

    history = {}
    for row in rows:
        deckle = row["deckle"]
        if deckle not in history:
            history[deckle] = {}

        history[deckle][row["bpro"]] = {
            "bpro": row["bpro"],
            "deckle_raw": row["deckle_raw"],
            "item_code": row["item_code"],
            "runs": row["runs"],
            "total_qty": row["total_qty"],
            "last_run": datetime.fromisoformat(row["last_run"]),
            "first_run": datetime.fromisoformat(row["first_run"]),
        }

    return history


def force_resync():
    """Force a re-sync on next access."""
    conn = _get_conn()
    conn.execute("DELETE FROM meta WHERE key='last_sync'")
    conn.execute("DELETE FROM pivot_cache")
    conn.commit()
    conn.close()


# ---- Pivot cache (30-min SQLite behind 5-min in-memory) ----

def save_pivot(key, json_data):
    conn = _get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO pivot_cache (key, data, updated_at) VALUES (?, ?, ?)",
        (key, json_data, datetime.now().isoformat())
    )
    conn.commit()
    conn.close()


def load_pivot(key, max_age_seconds=1800):
    conn = _get_conn()
    row = conn.execute("SELECT data, updated_at FROM pivot_cache WHERE key=?", (key,)).fetchone()
    conn.close()
    if not row:
        return None
    updated = datetime.fromisoformat(row["updated_at"])
    if (datetime.now() - updated).total_seconds() > max_age_seconds:
        return None
    return row["data"]


# Initialize DB on import
init_db()
