"""
Test script: Simulate Glue job bulk processing and roll call indexing.

Modes:
1. --simulate-bulk: Parse a local ZIP (simulates S3), extract bills with roll calls,
   stop after 2 files with votes, output CSV of table records, then run house-vote
   second pass (same API as Glue) to show remaining roll calls.

2. Single bill: python test_bill_roll_call.py 2189
   Fetch roll call data for one bill from Congress.gov API.

3. --all-rolls: Fetch all roll calls for congress/session via house-vote list API.

Usage:
  # Simulate Glue bulk flow (local ZIP, CSV output, second pass)
  python test_bill_roll_call.py --simulate-bulk
  python test_bill_roll_call.py --simulate-bulk --zip "BILLSTATUS-119-hr (3).zip"
  python test_bill_roll_call.py --simulate-bulk -o bills.csv --rolls-json rolls_second_pass.json

  # Single bill
  python test_bill_roll_call.py 2189 --congress 119 --type hr

  # All rolls (house-vote list API)
  python test_bill_roll_call.py --all-rolls --congress 119

  # Inspect house-vote members API response (raw JSON for schema debugging)
  python test_bill_roll_call.py --inspect-members --api-key YOUR_KEY
  python test_bill_roll_call.py --inspect-members --congress 119 --session 1 --roll 8 --api-key YOUR_KEY
"""

import argparse
import csv
import json
import os
import sys
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = "https://api.congress.gov/v3"

# Paste your Congress.gov API key here, or set CONGRESS_API_KEY env var.
# Get a key at: https://api.congress.gov/sign-up/
CONGRESS_API_KEY = "4ju6seBBsE2s3YfudxravIWoMJe0vtRKm4rTDyiX"  # <-- paste key here, or use --api-key
API_KEY = CONGRESS_API_KEY or os.environ.get("CONGRESS_API_KEY", "")

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# Default ZIP path (relative to script dir)
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ZIP = SCRIPT_DIR / "BILLSTATUS-119-hr (3).zip"

STATE_ABBR_TO_FULL = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia", "PR": "Puerto Rico", "VI": "U.S. Virgin Islands", "GU": "Guam",
    "AS": "American Samoa", "MP": "Northern Mariana Islands",
}
PARTY_TO_FULL = {"R": "Republican", "D": "Democratic", "I": "Independent", "ID": "Independent"}


def get_current_congress() -> int:
    y = datetime.now(timezone.utc).year
    return ((y - 1789) // 2) + 1


def _params(api_key: str) -> Dict[str, Any]:
    return {"api_key": api_key, "format": "json"}


def make_request(url: str, params: Dict[str, Any], retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Make API request with retry and exponential backoff for 429 (matches backfill_bill_text.make_api_request)."""
    import time
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 429:
                if attempt < retries - 1:
                    wait = max(5, (2**attempt) * RETRY_DELAY)
                    print(f"  Rate limited (429), waiting {wait}s...", file=sys.stderr)
                    time.sleep(wait)
                    continue
                print("  Rate limited (429) after retries.", file=sys.stderr)
                return None
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                print(f"Request failed: {e}", file=sys.stderr)
                return None
    return None


# ============================================================================
# Minimal XML parse (mirrors Glue parse_bill_xml for key fields)
# ============================================================================

def _elem_text(elem: Optional[ET.Element], tag: str) -> str:
    if elem is None:
        return ""
    child = elem.find(tag)
    if child is not None and child.text:
        return (child.text or "").strip()
    return ""


def parse_bill_xml_minimal(xml_content: bytes) -> Optional[Dict[str, Any]]:
    """
    Minimal parse of bill status XML. Extracts fields needed for table CSV output.
    Matches Glue glue_script.parse_bill_xml structure for recorded_votes, bill_id, etc.
    """
    try:
        root = ET.fromstring(xml_content)
        bill_elem = root.find("bill")
        if bill_elem is None:
            return None

        congress_elem = bill_elem.find("congress")
        congress = int(congress_elem.text) if congress_elem is not None and congress_elem.text else None
        t = bill_elem.find("type")
        bill_type_elem = t if t is not None else bill_elem.find("billType")
        bill_type = (bill_type_elem.text or "").upper().strip() if bill_type_elem is not None and bill_type_elem.text else None
        n = bill_elem.find("number")
        bill_number_elem = n if n is not None else bill_elem.find("billNumber")
        bill_number = (bill_number_elem.text or "").strip() if bill_number_elem is not None and bill_number_elem.text else None

        if not congress or not bill_type or not bill_number:
            return None

        bill_id = f"{congress}-{bill_type}-{bill_number}"

        # Title
        bill_title = ""
        titles_elem = bill_elem.find("titles")
        if titles_elem is not None:
            for item in titles_elem.findall("item") or []:
                tt = _elem_text(item, "titleType")
                tt_val = _elem_text(item, "title")
                if tt == "Display Title" and tt_val:
                    bill_title = tt_val
                    break
                if tt == "Official Title as Introduced" and tt_val and not bill_title:
                    bill_title = tt_val
        if not bill_title:
            bill_title = _elem_text(bill_elem, "title")

        # Dates
        intro_elem = bill_elem.find("introducedDate")
        introduced_date = (intro_elem.text or "").strip() if intro_elem is not None and intro_elem.text else _elem_text(bill_elem, "introducedDate")
        latest_action_elem = bill_elem.find("latestAction")
        latest_action_date = _elem_text(latest_action_elem, "actionDate") if latest_action_elem is not None else ""
        if not latest_action_date:
            actions_elem = bill_elem.find("actions")
            if actions_elem is not None:
                first = actions_elem.find("item")
                if first is not None:
                    latest_action_date = _elem_text(first, "actionDate")

        # Sponsor
        sponsor_full_name = ""
        sponsor_party = ""
        sponsors_elem = bill_elem.find("sponsors")
        if sponsors_elem is not None:
            item = sponsors_elem.find("item")
            if item is not None:
                fn = _elem_text(item, "firstName")
                ln = _elem_text(item, "lastName")
                full = _elem_text(item, "fullName")
                sponsor_full_name = full or f"{fn} {ln}".strip()
                sponsor_party = _elem_text(item, "party")

        # Recorded votes (mirror Glue: bill-level + actions/item/recordedVotes)
        def _norm_rv(rv: Dict) -> Tuple:
            return (
                (rv.get("chamber") or "").strip(),
                str(rv.get("rollNumber") or ""),
                str(rv.get("sessionNumber") or ""),
            )

        recorded_votes: List[Dict] = []
        seen_rv: Set[Tuple] = set()

        rv_elem = bill_elem.find("recordedVotes")
        if rv_elem is not None:
            for rv_item in rv_elem.findall("recordedVote") or []:
                rv = {}
                for tag in ("chamber", "congress", "date", "fullActionName", "rollNumber", "sessionNumber", "url"):
                    val = _elem_text(rv_item, tag)
                    if val and tag in ("rollNumber", "sessionNumber") and val.isdigit():
                        rv[tag] = int(val)
                    elif val:
                        rv[tag] = val
                if rv and _norm_rv(rv) not in seen_rv:
                    seen_rv.add(_norm_rv(rv))
                    recorded_votes.append(rv)

        actions_elem = bill_elem.find("actions")
        if actions_elem is not None:
            for action_item in actions_elem.findall("item") or []:
                rv_action = action_item.find("recordedVotes")
                if rv_action is not None:
                    for rv_item in rv_action.findall("recordedVote") or []:
                        rv = {}
                        for tag in ("chamber", "congress", "date", "fullActionName", "rollNumber", "sessionNumber", "url"):
                            val = _elem_text(rv_item, tag)
                            if val and tag in ("rollNumber", "sessionNumber") and val.isdigit():
                                rv[tag] = int(val)
                            elif val:
                                rv[tag] = val
                        if rv and _norm_rv(rv) not in seen_rv:
                            seen_rv.add(_norm_rv(rv))
                            recorded_votes.append(rv)

        has_roll_call = 1 if recorded_votes else 0
        return {
            "bill_id": bill_id,
            "congress": congress,
            "bill_type": bill_type,
            "bill_number": bill_number,
            "bill_title": bill_title,
            "introduced_date": introduced_date,
            "latest_action_date": latest_action_date,
            "sponsor_full_name": sponsor_full_name,
            "sponsor_party": sponsor_party,
            "has_roll_call": has_roll_call,
            "recorded_votes_json": json.dumps(recorded_votes) if recorded_votes else "",
            "roll_count": len(recorded_votes),
        }
    except ET.ParseError as e:
        print(f"XML parse error: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error parsing bill XML: {e}", file=sys.stderr)
        return None


def house_rolls_from_recorded_votes(recorded_votes_json: str) -> Set[Tuple[str, int, int]]:
    """Extract (congress_str, session, roll) for House votes. Matches roll_call_indexing._house_rolls_from_recorded_votes_json."""
    out: Set[Tuple[str, int, int]] = set()
    if not recorded_votes_json:
        return out
    try:
        raw = json.loads(recorded_votes_json)
    except json.JSONDecodeError:
        return out
    if not isinstance(raw, list):
        return out
    for v in raw:
        if not isinstance(v, dict):
            continue
        if (v.get("chamber") or "").strip().upper() != "HOUSE":
            continue
        congress = v.get("congress")
        sess, roll = v.get("sessionNumber"), v.get("rollNumber")
        if congress is None or sess is None or roll is None:
            continue
        try:
            out.add((str(congress), int(sess), int(roll)))
        except (ValueError, TypeError):
            pass
    return out


# ============================================================================
# Simulate bulk processing (mirrors Glue process_bulk_zip_file + second pass)
# ============================================================================

def run_simulate_bulk(
    zip_path: Path,
    stop_after_vote_files: int,
    congress: int,
    output_csv: Optional[str],
    output_rolls_json: Optional[str],
    api_key: str,
) -> None:
    """
    Parse ZIP, extract bills, stop after N files with votes. Output CSV. Run house-vote second pass.
    """
    if not zip_path.exists():
        print(f"ZIP not found: {zip_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Simulating Glue bulk flow")
    print(f"  ZIP: {zip_path}")
    print(f"  Stop after {stop_after_vote_files} file(s) with votes")
    print()

    with open(zip_path, "rb") as f:
        zip_content = f.read()

    zip_file = BytesIO(zip_content)
    with zipfile.ZipFile(zip_file, "r") as zf:
        file_list = zf.namelist()
        xml_files = [f for f in file_list if f.lower().endswith(".xml")]

    print(f"Found {len(xml_files)} XML file(s) in ZIP")
    bills: List[Dict] = []
    rolls_from_bills: Set[Tuple[str, int, int]] = set()
    files_with_votes = 0

    for filename in xml_files:
        try:
            zip_single = BytesIO(zip_content)
            with zipfile.ZipFile(zip_single, "r") as zf:
                xml_content = zf.read(filename)
        except Exception as e:
            print(f"  Failed to extract {filename}: {e}", file=sys.stderr)
            continue

        record = parse_bill_xml_minimal(xml_content)
        if not record:
            continue

        if record.get("has_roll_call"):
            files_with_votes += 1
            rolls_from_bills.update(
                house_rolls_from_recorded_votes(record.get("recorded_votes_json") or "")
            )

        bills.append(record)
        short_id = (record.get("bill_id") or "").replace("-", "")
        print(f"  Parsed {short_id} (has_roll_call={record.get('has_roll_call')}, rolls={record.get('roll_count', 0)})")

        if files_with_votes >= stop_after_vote_files:
            print(f"\nStopping: {stop_after_vote_files} file(s) with votes found.")
            break

    print(f"\nProcessed {len(bills)} bill(s), {len(rolls_from_bills)} roll(s) from bulk XML")

    # CSV output (table projection)
    csv_columns = [
        "bill_id", "congress", "bill_type", "bill_number", "bill_title",
        "introduced_date", "latest_action_date", "sponsor_full_name", "sponsor_party",
        "has_roll_call", "roll_count", "recorded_votes_summary",
    ]

    def csv_row(r: Dict) -> Dict[str, str]:
        rv_json = r.get("recorded_votes_json") or ""
        try:
            rv = json.loads(rv_json) if rv_json else []
            summary = "; ".join(
                f"House {v.get('sessionNumber')}/{v.get('rollNumber')}" for v in rv[:5]
            )
            if len(rv) > 5:
                summary += f" (+{len(rv)-5} more)"
        except json.JSONDecodeError:
            summary = ""
        return {
            "bill_id": str(r.get("bill_id", "")),
            "congress": str(r.get("congress", "")),
            "bill_type": str(r.get("bill_type", "")),
            "bill_number": str(r.get("bill_number", "")),
            "bill_title": (str(r.get("bill_title", "")) or "")[:200],
            "introduced_date": str(r.get("introduced_date", "")),
            "latest_action_date": str(r.get("latest_action_date", "")),
            "sponsor_full_name": str(r.get("sponsor_full_name", "")),
            "sponsor_party": str(r.get("sponsor_party", "")),
            "has_roll_call": str(r.get("has_roll_call", 0)),
            "roll_count": str(r.get("roll_count", 0)),
            "recorded_votes_summary": summary,
        }

    if output_csv:
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=csv_columns)
            w.writeheader()
            for r in bills:
                w.writerow(csv_row(r))
        print(f"Wrote CSV: {output_csv} ({len(bills)} rows)")
    else:
        print("\n--- CSV (first 5 rows) ---")
        w = csv.DictWriter(sys.stdout, fieldnames=csv_columns)
        w.writeheader()
        for r in bills[:5]:
            w.writerow(csv_row(r))
        if len(bills) > 5:
            print(f"... and {len(bills) - 5} more")

    # House-vote second pass (same as Glue roll_call_indexing.run_house_vote_second_pass)
    print("\n" + "=" * 60)
    print("House-vote second pass (API: same as Glue)")
    print("=" * 60)

    all_api_rolls: List[Dict] = []
    for session in (1, 2):
        vote_list = fetch_house_vote_list(congress, session, api_key, debug_keys=False)
        print(f"Congress {congress} Session {session}: house-vote list returned {len(vote_list)} roll(s)")
        for v in vote_list:
            vc = dict(v)
            vc.setdefault("sessionNumber", session)
            all_api_rolls.append(vc)

    # Rolls from API not in bulk
    api_roll_keys: Set[Tuple[str, int, int]] = set()
    for v in all_api_rolls:
        rn = v.get("rollCallNumber") or v.get("rollNumber")
        sn = v.get("sessionNumber")
        if rn is not None and sn is not None:
            api_roll_keys.add((str(congress), int(sn), int(rn)))

    second_pass_rolls = api_roll_keys - rolls_from_bills
    print(f"\nRolls from bulk XML: {len(rolls_from_bills)}")
    print(f"Rolls from house-vote API: {len(api_roll_keys)}")
    print(f"Second pass would fetch: {len(second_pass_rolls)} (not in bulk)")

    if second_pass_rolls:
        sample = sorted(second_pass_rolls)[:10]
        print(f"Sample second-pass rolls (congress, session, roll): {sample}")

    second_pass_detail = []
    for v in all_api_rolls:
        rn = v.get("rollCallNumber") or v.get("rollNumber")
        sn = v.get("sessionNumber")
        if rn is None or sn is None:
            continue
        key = (str(congress), int(sn), int(rn))
        if key in second_pass_rolls:
            second_pass_detail.append({
                "congress": congress,
                "session": int(sn),
                "roll": int(rn),
                "legislationUrl": v.get("legislationUrl"),
                "bill_id_associated": _bill_id_from_house_vote_item(v),
            })

    if output_rolls_json:
        with open(output_rolls_json, "w", encoding="utf-8") as f:
            json.dump({
                "congress": congress,
                "rolls_from_bulk": [{"congress": c, "session": s, "roll": r} for c, s, r in sorted(rolls_from_bills)],
                "rolls_from_api": [{"congress": c, "session": s, "roll": r} for c, s, r in sorted(api_roll_keys)],
                "second_pass_rolls": second_pass_detail,
            }, f, indent=2, default=str)
        print(f"Wrote second-pass rolls: {output_rolls_json}")


def _bill_id_from_house_vote_item(v: Dict) -> str:
    """Build bill_id from house-vote list item. Matches roll_call_indexing._bill_id_from_house_vote_item."""
    congress = v.get("congress")
    leg_type = (v.get("legislationType") or "").strip().upper()
    leg_num = (v.get("legislationNumber") or "").strip()
    if congress is not None and leg_type and leg_num:
        return f"{congress}-{leg_type}-{leg_num}"
    return ""


# ============================================================================
# House-vote API (matches test script + Glue roll_call_indexing)
# ============================================================================

def fetch_house_vote_list(
    congress: int, session: int, api_key: str, debug_keys: bool = False
) -> List[Dict]:
    """
    GET /house-vote/{congress}/{session} - matches backfill_bill_text._fetch_house_vote_list.
    Congress.gov v3 may wrap the list in 'votes', 'houseVotes', 'results', or 'items'.
    """
    out: List[Dict] = []
    offset, limit = 0, 250
    _logged_keys = False
    while True:
        url = f"{API_BASE_URL}/house-vote/{congress}/{session}"
        params = {**_params(api_key), "offset": offset, "limit": limit}
        data = make_request(url, params)
        if not data:
            break
        items = None
        if isinstance(data, dict):
            items = data.get("votes") or data.get("houseVotes") or data.get("results") or data.get("items")
            if isinstance(items, dict):
                items = items.get("item", items.get("vote", items.get("votes", [])))
            if not isinstance(items, list) and isinstance(data.get("houseVote"), dict):
                items = data["houseVote"].get("votes") or data["houseVote"].get("item")
            if not isinstance(items, list):
                for key, val in data.items():
                    if key in ("pagination", "request"):
                        continue
                    if isinstance(val, list) and val and isinstance(val[0], dict):
                        if "rollCallNumber" in val[0] or "legislationNumber" in val[0] or "rollNumber" in val[0]:
                            items = val
                            break
            if not isinstance(items, list) and offset == 0 and not _logged_keys:
                print(f"  [DEBUG] House-vote list response keys (Congress {congress} Session {session}): {list(data.keys())}", file=sys.stderr)
                _logged_keys = True
            if not isinstance(items, list) and isinstance(data, dict) and "rollCallNumber" in data:
                items = [data]
        if isinstance(items, list):
            out.extend(items)
        elif isinstance(data, list):
            out.extend(data)
        elif isinstance(data, dict) and "rollCallNumber" in data:
            out.append(data)
        if not isinstance(data, dict):
            break
        num = len(items) if isinstance(items, list) else 0
        if num < limit:
            break
        offset += limit
    return out


def fetch_house_vote_members(congress: int, session: int, roll_number: int, api_key: str) -> List[Dict]:
    all_members = []
    offset, limit = 0, 250
    while True:
        url = f"{API_BASE_URL}/house-vote/{congress}/{session}/{roll_number}/members"
        params = {**_params(api_key), "offset": offset, "limit": limit}
        data = make_request(url, params)
        if not data:
            break
        results = data.get("results") or data.get("members") or data.get("memberVotes") if isinstance(data, dict) else None
        if not isinstance(results, list) and isinstance(data.get("houseVote"), dict):
            results = data["houseVote"].get("results") or data["houseVote"].get("members")
        if isinstance(results, list):
            all_members.extend(results)
        if not isinstance(data, dict):
            break
        pagination = data.get("pagination") or {}
        count = pagination.get("count", 0) if isinstance(pagination, dict) else 0
        if count and offset + limit >= count:
            break
        if not (isinstance(results, list) and len(results) == limit):
            break
        offset += limit
    return all_members


# ============================================================================
# Legacy modes: single bill, --all-rolls
# ============================================================================

def fetch_bill(congress: int, bill_type: str, bill_number: str, api_key: str) -> Optional[Dict]:
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}"
    data = make_request(url, _params(api_key))
    if data and isinstance(data, dict):
        return data.get("bill", data)
    return data


def fetch_bill_actions(congress: int, bill_type: str, bill_number: str, api_key: str) -> List[Dict]:
    actions = []
    offset, limit = 0, 250
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/actions"
        params = {**_params(api_key), "offset": offset, "limit": limit}
        data = make_request(url, params)
        if not data:
            break
        raw = data.get("actions") if isinstance(data, dict) else None
        items = raw.get("item", []) if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
        if not items:
            break
        actions.extend(items)
        pagination = (data.get("actions") or {}).get("pagination") if isinstance(data, dict) else {}
        count = (pagination or {}).get("count", 0)
        if count and offset + limit >= count:
            break
        offset += limit
    return actions


def extract_recorded_votes_from_actions(actions: List[Dict]) -> List[Dict]:
    seen = set()
    out = []
    for action in actions:
        votes = action.get("recordedVotes") or action.get("recordedvotes")
        if not isinstance(votes, list):
            continue
        for v in votes:
            if not isinstance(v, dict):
                continue
            chamber = (v.get("chamber") or "").strip()
            c, sess, roll = v.get("congress"), v.get("sessionNumber"), v.get("rollNumber")
            key = (chamber, c, sess, roll)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "chamber": chamber, "congress": c, "sessionNumber": sess, "rollNumber": roll,
                "date": v.get("date"), "url": v.get("url"), "fullActionName": v.get("fullActionName"),
            })
    return out


def build_roll_call_data_for_bill(
    congress: int, bill_type: str, bill_number: str, api_key: str, debug_empty_members: bool = False
) -> Dict[str, Any]:
    bill = fetch_bill(congress, bill_type, bill_number, api_key)
    if not bill:
        return {"error": "Bill not found", "congress": congress, "billType": bill_type, "billNumber": bill_number}
    actions = fetch_bill_actions(congress, bill_type, bill_number, api_key)
    recorded_votes = extract_recorded_votes_from_actions(actions)
    roll_calls = []
    for rv in recorded_votes:
        chamber = (rv.get("chamber") or "").strip()
        c, sess, roll = rv.get("congress"), rv.get("sessionNumber"), rv.get("rollNumber")
        entry = {
            "chamber": chamber, "congress": c, "sessionNumber": sess, "rollNumber": roll,
            "date": rv.get("date"), "url": rv.get("url"), "fullActionName": rv.get("fullActionName"),
            "memberVotes": None,
        }
        if chamber == "House" and c is not None and sess is not None and roll is not None:
            members = fetch_house_vote_members(int(c), int(sess), int(roll), api_key)
            entry["memberVotes"] = members
        roll_calls.append(entry)
    return {
        "bill_id": f"{congress}-{bill_type.upper()}-{bill_number}",
        "congress": congress, "billType": bill_type.upper(), "billNumber": str(bill_number),
        "billTitle": (bill.get("title") or "").strip() or (bill.get("titles", {}).get("title") or ""),
        "rollCallVotes": roll_calls,
        "rollCallCount": len(roll_calls),
    }


def format_vote_list_table(member_votes: List[Dict]) -> List[str]:
    if not member_votes:
        return []
    party_f = lambda p: PARTY_TO_FULL.get((p or "").strip().upper(), (p or "").strip())
    state_f = lambda s: STATE_ABBR_TO_FULL.get((s or "").strip().upper(), (s or "").strip())
    rows = []
    for m in member_votes:
        last = (m.get("lastName") or "").strip()
        first = (m.get("firstName") or "").strip()
        party = (m.get("voteParty") or "").strip().upper() or "?"
        state = (m.get("voteState") or "").strip().upper() or "?"
        vote = (m.get("voteCast") or "").strip()
        name_part = f"{last}, {first} [{party}-{state}]"
        rows.append((name_part, party_f(party), state_f(state), vote))
    rows.sort(key=lambda r: (r[0].split(",")[0].strip().lower(), r[0]))
    return [f"{n}\t{p}\t{s}\t{v}" for n, p, s, v in rows]


def run_all_rolls(
    congress: int, session: Optional[int], api_key: str, debug: bool, output_path: Optional[str]
) -> None:
    sessions = [session] if session is not None else [1, 2]
    all_votes: List[Dict] = []
    for sess in sessions:
        vote_list = fetch_house_vote_list(congress, sess, api_key, debug_keys=debug)
        print(f"Congress {congress} Session {sess}: house-vote list returned {len(vote_list)} roll(s).")
        for v in vote_list:
            vc = dict(v)
            vc.setdefault("sessionNumber", sess)
            all_votes.append(vc)
    print(f"Total roll calls: {len(all_votes)}")
    if output_path:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"congress": congress, "sessions": sessions, "rollCount": len(all_votes), "votes": all_votes}, f, indent=2, default=str)
        print(f"Wrote {output_path}")


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate Glue bulk processing + roll call indexing, or fetch roll call data for a bill."
    )
    parser.add_argument("bill_number", type=str, nargs="?", default=None, help="Bill number (e.g. 2189)")
    parser.add_argument("--congress", type=int, default=None, help="Congress number")
    parser.add_argument("--type", dest="bill_type", type=str, default="hr", help="Bill type")
    parser.add_argument("--api-key", type=str, default="", help="Congress.gov API key")
    parser.add_argument("-o", "--output", type=str, default=None, help="Write JSON/CSV to file")
    parser.add_argument("--votes-file", type=str, default=None, help="Write vote list to TSV")
    parser.add_argument("--debug", action="store_true", help="Debug output")
    parser.add_argument("--all-rolls", action="store_true", help="Fetch all roll calls (house-vote list API)")
    parser.add_argument("--session", type=int, default=None, choices=[1, 2], help="Session for --all-rolls")
    parser.add_argument(
        "--inspect-members",
        action="store_true",
        help="Call house-vote/{congress}/{session}/{roll}/members and print raw JSON (for schema inspection)",
    )
    parser.add_argument("--roll", type=int, default=8, help="Roll number for --inspect-members (default: 8)")

    parser.add_argument(
        "--simulate-bulk",
        action="store_true",
        help="Parse local ZIP (simulate S3), stop after 2 files with votes, output CSV, run second pass",
    )
    parser.add_argument(
        "--zip",
        type=str,
        default=None,
        help=f"ZIP path for --simulate-bulk (default: {DEFAULT_ZIP.name})",
    )
    parser.add_argument(
        "--stop-after",
        type=int,
        default=2,
        help="Stop after N files with votes (default: 2)",
    )
    parser.add_argument(
        "--rolls-json",
        type=str,
        default=None,
        help="Write second-pass rolls JSON for --simulate-bulk",
    )

    args = parser.parse_args()
    congress = args.congress or get_current_congress()
    api_key = (args.api_key or CONGRESS_API_KEY or os.environ.get("CONGRESS_API_KEY") or "").strip()

    if args.simulate_bulk:
        if not api_key:
            print("Set CONGRESS_API_KEY or --api-key for house-vote second pass API calls.", file=sys.stderr)
            sys.exit(1)
        zip_path = Path(args.zip) if args.zip else DEFAULT_ZIP
        run_simulate_bulk(
            zip_path=zip_path,
            stop_after_vote_files=args.stop_after,
            congress=congress,
            output_csv=args.output,
            output_rolls_json=args.rolls_json,
            api_key=api_key,
        )
        return

    if args.all_rolls:
        if not api_key:
            print("Set CONGRESS_API_KEY or --api-key", file=sys.stderr)
            sys.exit(1)
        run_all_rolls(congress, args.session, api_key, args.debug, args.output)
        return

    if args.inspect_members:
        sess = args.session if args.session is not None else 1
        roll = args.roll
        if not api_key:
            print("Set CONGRESS_API_KEY or --api-key for API call.", file=sys.stderr)
            sys.exit(1)
        url = f"{API_BASE_URL}/house-vote/{congress}/{sess}/{roll}/members"
        params = {"api_key": api_key, "format": "json", "limit": 5}
        print(f"GET {url}", file=sys.stderr)
        data = make_request(url, params)
        if data is None:
            print("Request failed.", file=sys.stderr)
            sys.exit(1)
        print(json.dumps(data, indent=2, default=str))
        return

    if not args.bill_number:
        parser.error("bill_number required unless --simulate-bulk or --all-rolls")
    if not api_key:
        print("Set CONGRESS_API_KEY or --api-key", file=sys.stderr)
        sys.exit(1)

    print(f"Bill: Congress {congress}, {args.bill_type.upper()} {args.bill_number}")
    result = build_roll_call_data_for_bill(congress, args.bill_type, args.bill_number, api_key, debug_empty_members=args.debug)
    if result.get("error"):
        print(result["error"])
        sys.exit(2)
    print(f"Roll calls found: {result.get('rollCallCount', 0)}")
    for rc in result.get("rollCallVotes", []):
        mv = rc.get("memberVotes") or []
        print(f"  - {rc.get('chamber')} roll {rc.get('rollNumber')} (session {rc.get('sessionNumber')}): {len(mv)} member votes")
    all_lines = []
    for rc in result.get("rollCallVotes", []):
        all_lines.extend(format_vote_list_table(rc.get("memberVotes") or []))
    if all_lines:
        text = "\n".join(all_lines)
        if args.votes_file:
            with open(args.votes_file, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"Wrote vote list to {args.votes_file}")
        else:
            print("\n--- Vote list ---\n" + text + "\n---")
    out = json.dumps(result, indent=2, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out)
        print(f"Wrote {args.output}")
    else:
        print(out)


if __name__ == "__main__":
    main()
