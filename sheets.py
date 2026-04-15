"""Google Sheets reader for Board Planner — read-only, with in-memory cache."""

import os
import re
import time
from datetime import datetime, timedelta

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


# ---- Paper config parsing ----

def extract_paper(board_item):
    """Extract the paper/GSM portion from a board item string.

    e.g. 'BD-AB FLUTE-230vk28_140sk18_140sk18_140sk18_230vk28-18-1185'
      -> '230vk28_140sk18_140sk18_140sk18_230vk28'
    """
    if not board_item:
        return ""
    # Pattern: BD-{FLUTE}-{PAPER}-{deckle}-{size}
    # Paper portion is between the flute type and the numeric deckle/size
    parts = board_item.split("-")
    # Find the paper portion — it's the segment with underscores
    for part in parts:
        if "_" in part and any(c.isdigit() for c in part):
            return part.lower()
    return ""


def extract_flute(board_item):
    """Extract flute type from board item string.

    e.g. 'BD-AB FLUTE-...' -> 'AB FLUTE'
    """
    if not board_item:
        return ""
    match = re.match(r'BD-([A-Z\s]+FLUTE)', board_item, re.IGNORECASE)
    return match.group(1).upper() if match else ""


# ---- BPRO master lookup ----

def _build_bpro_master():
    """Build BPRO -> board_item + customer lookup from BPRO_SHEET."""
    rows = _read_sheet("BPRO_SHEET")
    lookup = {}
    for row in rows[1:]:
        bpro = _col(row, 4)  # Column E = BPRO-xxx
        board_item = _col(row, 2)  # Column C = Item (Boards)To Manufacture
        customer = _col(row, 13)  # Column N = CUSTOMER
        item_name = _col(row, 17)  # Column R = Item Name
        ipro = _col(row, 14)  # Column O = IPRO
        running_name = _col(row, 41)  # Column AP = Running Name
        if bpro and board_item:
            lookup[bpro] = {
                "board_item": board_item,
                "customer": customer,
                "item_name": item_name,
                "ipro": ipro,
                "running_name": running_name,
            }
    return lookup


def get_bpro_master():
    return _cached("bpro_master", _build_bpro_master)


# ---- History (App Data last 90 days) ----

def _parse_history():
    """Parse App Data for last 90 days of production, grouped by deckle."""
    rows = _read_sheet("App Data")
    if len(rows) < 2:
        return {}

    cutoff = datetime.now() - timedelta(days=90)
    history = {}  # deckle -> {board_item -> {details}}

    for row in rows[1:]:
        if len(row) < 7 or not _col(row, 1):
            continue

        # Parse timestamp
        ts_str = _col(row, 0)
        try:
            ts = datetime.strptime(ts_str.split(".")[0], "%d/%m/%Y %H:%M:%S")
        except ValueError:
            try:
                ts = datetime.strptime(ts_str.split(".")[0], "%m/%d/%Y %H:%M:%S")
            except ValueError:
                continue

        if ts < cutoff:
            continue

        bpro = _col(row, 1)
        deckle_raw = _col(row, 6)
        item_code = _col(row, 18) if len(row) > 18 else ""
        qty_str = _col(row, 3)

        try:
            qty = int(float(qty_str)) if qty_str else 0
        except (ValueError, TypeError):
            qty = 0

        # Normalize deckle to nearest integer for grouping
        try:
            deckle_float = float(deckle_raw)
            deckle = str(int(round(deckle_float)))
        except (ValueError, TypeError):
            deckle = deckle_raw

        if not deckle:
            continue

        if deckle not in history:
            history[deckle] = {}

        # Use BPRO as key since the same BPRO = same board
        if bpro not in history[deckle]:
            history[deckle][bpro] = {
                "bpro": bpro,
                "deckle_raw": deckle_raw,
                "item_code": item_code,
                "runs": 0,
                "total_qty": 0,
                "last_run": ts,
                "first_run": ts,
            }

        entry = history[deckle][bpro]
        entry["runs"] += 1
        entry["total_qty"] += qty
        if ts > entry["last_run"]:
            entry["last_run"] = ts
        if ts < entry["first_run"]:
            entry["first_run"] = ts

    return history


def get_history():
    return _cached("history", _parse_history)


# ---- Deckle detail with 4 sections ----

def get_deckle_detail(deckle, reference_bpro=None):
    """Get detailed view for a deckle size with 4 sections.

    Args:
        deckle: deckle size string (e.g. "42")
        reference_bpro: optional BPRO to use as the paper reference.
                        If None, uses the first pending job's paper.

    Returns dict with:
        reference_paper: the paper config used for same/diff comparison
        pending_same_paper: pending BPROs with matching paper
        pending_diff_paper: pending BPROs with different paper
        missing_same_paper: historical boards (no BPRO) with matching paper
        missing_diff_paper: historical boards (no BPRO) with different paper
    """
    # 1. Get all pending jobs for this deckle
    deckle_data = get_deckle_jobs()
    pending_jobs = []
    for group in deckle_data["groups"]:
        if group["deckle"] == deckle:
            pending_jobs = group["jobs"]
            break

    pending_bpro_set = {j["bpro"] for j in pending_jobs}

    # 2. Determine reference paper config
    ref_paper = ""
    if reference_bpro:
        for j in pending_jobs:
            if j["bpro"] == reference_bpro:
                ref_paper = extract_paper(j["board_item"])
                break
    if not ref_paper and pending_jobs:
        ref_paper = extract_paper(pending_jobs[0]["board_item"])

    # 3. Split pending jobs by paper match
    pending_same = []
    pending_diff = []
    for job in pending_jobs:
        job_paper = extract_paper(job["board_item"])
        entry = {**job, "paper": job_paper, "flute": extract_flute(job["board_item"])}
        if job_paper and ref_paper and job_paper == ref_paper:
            pending_same.append(entry)
        else:
            pending_diff.append(entry)

    # 4. Get history for this deckle
    history = get_history()
    bpro_master = get_bpro_master()

    # Find matching deckle in history (try exact and rounded)
    hist_entries = history.get(deckle, {})

    # 5. Build missing opportunities — BPROs in history but NOT in pending
    missing_same = []
    missing_diff = []
    seen_board_items = set()

    for bpro, entry in hist_entries.items():
        if bpro in pending_bpro_set:
            continue  # already has a pending BPRO

        # Look up board item from master
        master = bpro_master.get(bpro, {})
        board_item = master.get("board_item", "")
        if not board_item:
            continue

        # Deduplicate by board_item (same board can have multiple historical BPROs)
        if board_item in seen_board_items:
            continue
        seen_board_items.add(board_item)

        paper = extract_paper(board_item)
        flute = extract_flute(board_item)

        opp = {
            "bpro": bpro,
            "board_item": board_item,
            "item_name": master.get("item_name", entry.get("item_code", "")),
            "customer": master.get("customer", ""),
            "running_name": master.get("running_name", ""),
            "ipro": master.get("ipro", ""),
            "paper": paper,
            "flute": flute,
            "runs_90d": entry["runs"],
            "total_qty_90d": entry["total_qty"],
            "last_run": entry["last_run"].strftime("%d/%m/%Y"),
            "avg_qty": round(entry["total_qty"] / entry["runs"]) if entry["runs"] else 0,
        }

        if paper and ref_paper and paper == ref_paper:
            missing_same.append(opp)
        else:
            missing_diff.append(opp)

    # Sort: most recent first for missing, by qty for pending
    missing_same.sort(key=lambda x: x["runs_90d"], reverse=True)
    missing_diff.sort(key=lambda x: x["runs_90d"], reverse=True)

    return {
        "deckle": deckle,
        "reference_paper": ref_paper,
        "reference_bpro": reference_bpro or (pending_jobs[0]["bpro"] if pending_jobs else ""),
        "pending_same_paper": pending_same,
        "pending_diff_paper": pending_diff,
        "missing_same_paper": missing_same,
        "missing_diff_paper": missing_diff,
    }
