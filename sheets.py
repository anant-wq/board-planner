"""Google Sheets reader for Board Planner — read-only, with in-memory cache."""

import os
import time

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
CREDS_FILE = os.path.join(os.path.dirname(__file__), "credentials.json")

BOARD_JOB_CARD_ID = "1ksKGJRzBvyzgwhXu5ZD9QfM1f1Hfq9MA8Ons-ris99g"

CACHE_TTL = 300  # 5 minutes
_cache = {}


def _get_service():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def _col(row, idx, default=""):
    """Safe column access — sheets API omits trailing empty cells."""
    return row[idx].strip() if len(row) > idx and row[idx] else default


def _read_sheet(sheet_name, spreadsheet_id=BOARD_JOB_CARD_ID):
    """Read all rows from a sheet tab. Returns list of lists."""
    service = _get_service()
    data = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'"
    ).execute()
    return data.get("values", [])


def _cached(key, fetcher):
    """Return cached data or fetch fresh."""
    now = time.time()
    if key in _cache and now - _cache[key]["ts"] < CACHE_TTL:
        return _cache[key]["data"]
    data = fetcher()
    _cache[key] = {"data": data, "ts": now}
    return data


def clear_cache():
    _cache.clear()


# ---- Deckle-pivot Auto line ----

def _parse_deckle_pivot():
    rows = _read_sheet("deckle-pivot Auto line")
    if len(rows) < 2:
        return {"groups": [], "total_jobs": 0}

    groups = {}
    current_deckle = ""

    for row in rows[1:]:  # skip header
        if not row or not _col(row, 1):  # skip empty rows (no BPRO)
            continue

        deckle_val = _col(row, 0)
        if deckle_val:
            current_deckle = deckle_val

        job = {
            "bpro": _col(row, 1),
            "board_item": _col(row, 2),
            "ipro": _col(row, 3),
            "item_name": _col(row, 4),
            "customer": _col(row, 5),
            "running_name": _col(row, 6),
            "qty": _col(row, 7),
            "pro_date": _col(row, 8),
        }

        if current_deckle not in groups:
            groups[current_deckle] = []
        groups[current_deckle].append(job)

    # Sort groups by deckle size (numeric)
    sorted_groups = []
    for deckle in sorted(groups.keys(), key=lambda d: float(d) if d else 0):
        jobs = groups[deckle]
        total_qty = 0
        for j in jobs:
            try:
                total_qty += int(j["qty"])
            except (ValueError, TypeError):
                pass
        sorted_groups.append({
            "deckle": deckle,
            "jobs": jobs,
            "job_count": len(jobs),
            "total_qty": total_qty,
        })

    total_jobs = sum(g["job_count"] for g in sorted_groups)
    return {"groups": sorted_groups, "total_jobs": total_jobs}


def get_deckle_jobs():
    return _cached("deckle", _parse_deckle_pivot)


# ---- Client-pivot autoline ----

def _parse_client_pivot():
    rows = _read_sheet("client-pivot autoline")
    if len(rows) < 2:
        return {"groups": [], "total_jobs": 0}

    groups = {}
    current_client = ""

    for row in rows[1:]:
        if not row or not _col(row, 2):  # skip empty rows (no BPRO)
            continue

        client_val = _col(row, 0)
        if client_val:
            current_client = client_val

        job = {
            "client": current_client,
            "pro_date": _col(row, 1),
            "bpro": _col(row, 2),
            "board_item": _col(row, 3),
            "item_name": _col(row, 4),
            "running_name": _col(row, 5),
            "qty": _col(row, 6),
            "deckle": _col(row, 7),
            "poc_name": _col(row, 9),
        }

        if current_client not in groups:
            groups[current_client] = []
        groups[current_client].append(job)

    sorted_groups = []
    for client in sorted(groups.keys()):
        jobs = groups[client]
        total_qty = 0
        for j in jobs:
            try:
                total_qty += int(j["qty"])
            except (ValueError, TypeError):
                pass
        sorted_groups.append({
            "client": client,
            "jobs": jobs,
            "job_count": len(jobs),
            "total_qty": total_qty,
        })

    total_jobs = sum(g["job_count"] for g in sorted_groups)
    return {"groups": sorted_groups, "total_jobs": total_jobs}


def get_client_jobs():
    return _cached("client", _parse_client_pivot)


# ---- Export helper ----

def get_jobs_for_export(bpro_list):
    """Given a list of BPRO numbers, return job dicts from deckle-pivot data."""
    data = get_deckle_jobs()
    bpro_set = set(bpro_list)
    results = []
    for group in data["groups"]:
        for job in group["jobs"]:
            if job["bpro"] in bpro_set:
                results.append({
                    "deckle": group["deckle"],
                    "bpro": job["bpro"],
                    "board_item": job["board_item"],
                    "ipro": job["ipro"],
                    "item_name": job["item_name"],
                    "customer": job["customer"],
                    "running_name": job["running_name"],
                    "qty": job["qty"],
                })
    return results
