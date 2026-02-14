"""
Test script: fetch roll call (vote) data for a single bill from Congress.gov API.

API endpoints used for roll call data:
- GET /bill/{congress}/{billType}/{billNumber}/actions
  Returns list of actions; actions with roll calls include "recordedVotes" array
  (chamber, congress, sessionNumber, rollNumber, date, url, fullActionName).
- GET /house-vote/{congress}/{session}/{voteNumber}
  House roll call metadata (result, voteQuestion, voteType, legislationUrl).
- GET /house-vote/{congress}/{session}/{voteNumber}/members
  How each House member voted: bioguideID, firstName, lastName, voteCast (Yea/Nay/etc),
  voteParty, voteState. (Senate member-level votes are not in the API; use actions
  recordedVotes for Senate roll metadata + url to senate.gov.)

Use this to verify the pipeline for adding roll call data to the bills table so the
details page can show which politicians voted which way on a bill.

Usage:
  python test_bill_roll_call.py 2189
  python test_bill_roll_call.py 2189 --congress 119 --type hr
  python test_bill_roll_call.py 2189 -o roll_call_2189.json

  # Fetch all roll calls for a congress/session (matches Glue house-vote list logic)
  python test_bill_roll_call.py --all-rolls --congress 119
  python test_bill_roll_call.py --all-rolls --congress 119 --session 1
  python test_bill_roll_call.py --all-rolls --congress 119 -o all_rolls_119.json
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = "https://api.congress.gov/v3"
# Set via CONGRESS_API_KEY env var or pass --api-key on the command line (do not commit keys)
API_KEY = "jVFi0sHwg2iolTUws0lSj5r0tHqfh98bcXnTiEAX"

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# State abbreviation -> full name (for vote list display)
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
    "DC": "District of Columbia", "PR": "Puerto Rico", "VI": "U.S. Virgin Islands", "GU": "Guam", "AS": "American Samoa", "MP": "Northern Mariana Islands",
}

PARTY_TO_FULL = {"R": "Republican", "D": "Democratic", "I": "Independent", "ID": "Independent"}


def get_current_congress() -> int:
    """Current Congress number (e.g. 119)."""
    y = datetime.now(timezone.utc).year
    return ((y - 1789) // 2) + 1


def _params(api_key: str) -> Dict[str, Any]:
    return {"api_key": api_key, "format": "json"}


def make_request(url: str, params: Dict[str, Any], retries: int = MAX_RETRIES) -> Optional[Dict]:
    """GET JSON with retries."""
    import time
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                print(f"Request failed: {e}", file=sys.stderr)
                return None
    return None


def fetch_bill(congress: int, bill_type: str, bill_number: str, api_key: str) -> Optional[Dict]:
    """GET /bill/{congress}/{billType}/{billNumber}."""
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}"
    data = make_request(url, _params(api_key))
    if data and isinstance(data, dict):
        return data.get("bill", data)
    return data


def fetch_bill_actions(congress: int, bill_type: str, bill_number: str, api_key: str) -> List[Dict]:
    """GET /bill/{congress}/{billType}/{billNumber}/actions (all pages)."""
    actions = []
    offset = 0
    limit = 250
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/actions"
        params = {**_params(api_key), "offset": offset, "limit": limit}
        data = make_request(url, params)
        if not data:
            break
        raw = data.get("actions") if isinstance(data, dict) else None
        if isinstance(raw, dict):
            items = raw.get("item", [])
        elif isinstance(raw, list):
            items = raw
        else:
            items = []
        if not items:
            break
        actions.extend(items)
        if isinstance(data, dict):
            actions_container = data.get("actions")
            pagination = actions_container.get("pagination") if isinstance(actions_container, dict) else data.get("pagination") or {}
        else:
            pagination = {}
        count = pagination.get("count", 0) if isinstance(pagination, dict) else 0
        if count and offset + limit >= count:
            break
        offset += limit
    return actions


def extract_recorded_votes_from_actions(actions: List[Dict]) -> List[Dict]:
    """Collect unique recordedVotes from bill actions. One action can have multiple votes."""
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
            congress = v.get("congress")
            session = v.get("sessionNumber")
            roll = v.get("rollNumber")
            key = (chamber, congress, session, roll)
            if key in seen:
                continue
            seen.add(key)
            out.append({
                "chamber": chamber,
                "congress": congress,
                "sessionNumber": session,
                "rollNumber": roll,
                "date": v.get("date"),
                "url": v.get("url"),
                "fullActionName": v.get("fullActionName"),
            })
    return out


def fetch_house_vote_list(congress: int, session: int, api_key: str, debug_keys: bool = False) -> List[Dict]:
    """
    GET /house-vote/{congress}/{session} - list all roll call votes for that Congress and session.
    Returns list of votes (rollCallNumber, sessionNumber, congress, legislationUrl, etc.). Paginated (limit 250).
    Matches Glue backfill_bill_text._fetch_house_vote_list: tries votes, houseVotes, results, items;
    fallback scan for list of dicts with rollCallNumber/legislationNumber/rollNumber; single-object wrap; stop when page < limit.
    """
    out: List[Dict] = []
    offset = 0
    limit = 250
    logged_keys = False
    while True:
        url = f"{API_BASE_URL}/house-vote/{congress}/{session}"
        params = {**_params(api_key), "format": "json", "offset": offset, "limit": limit}
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
            if not isinstance(items, list) and offset == 0 and (debug_keys or not logged_keys):
                print(f"      [debug] house-vote list response keys (Congress {congress} Session {session}): {list(data.keys())}", file=sys.stderr)
                logged_keys = True
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
        num_items = len(items) if isinstance(items, list) else 0
        if num_items < limit:
            break
        offset += limit
    return out


def fetch_house_vote_detail(congress: int, session: int, roll_number: int, api_key: str) -> Optional[Dict]:
    """GET /house-vote/{congress}/{session}/{voteNumber} (metadata)."""
    url = f"{API_BASE_URL}/house-vote/{congress}/{session}/{roll_number}"
    params = {**_params(api_key), "format": "json"}
    return make_request(url, params)


def fetch_house_vote_members(congress: int, session: int, roll_number: int, api_key: str) -> List[Dict]:
    """
    GET /house-vote/{congress}/{session}/{voteNumber}/members
    Matches Glue backfill_bill_text._fetch_house_vote_members: Congress.gov v3 returns "results" array
    (bioguideID, firstName, lastName, voteCast, voteParty, voteState). Try results first, then members, then legacy wrappers.
    Paginated (limit 250).
    """
    all_members = []
    offset = 0
    limit = 250
    while True:
        url = f"{API_BASE_URL}/house-vote/{congress}/{session}/{roll_number}/members"
        params = {**_params(api_key), "format": "json", "offset": offset, "limit": limit}
        data = make_request(url, params)
        if not data:
            break
        results = None
        if isinstance(data, dict):
            results = data.get("results") or data.get("members") or data.get("memberVotes")
            if not isinstance(results, list):
                raw = data.get("houseRollCallVoteMemberVotes")
                if isinstance(raw, list):
                    results = raw
                elif isinstance(raw, dict):
                    for key in ("item", "memberVote", "memberVotes", "houseRollCallVoteMemberVote"):
                        val = raw.get(key)
                        if isinstance(val, list):
                            results = val
                            break
                        if isinstance(val, dict) and (val.get("voteCast") or val.get("bioguideID")):
                            results = [val]
                            break
                    if results is None and raw:
                        for _k, val in raw.items():
                            if isinstance(val, list) and val and isinstance(val[0], dict):
                                if "voteCast" in val[0] or "bioguideID" in val[0]:
                                    results = val
                                    break
            if not isinstance(results, list) and isinstance(data.get("houseVote"), dict):
                h = data["houseVote"]
                results = h.get("results") or h.get("members")
            if not isinstance(results, list):
                for key in ("voteMembers", "items", "votes"):
                    cand = data.get(key)
                    if isinstance(cand, list) and cand and isinstance(cand[0], dict):
                        if "voteCast" in cand[0] or "bioguideID" in cand[0]:
                            results = cand
                            break
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            if "voteCast" in data[0] or "bioguideID" in data[0]:
                results = data
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


def build_roll_call_data_for_bill(
    congress: int,
    bill_type: str,
    bill_number: str,
    api_key: str,
    debug_empty_members: bool = False,
) -> Dict[str, Any]:
    """
    For a given bill: get actions -> recordedVotes -> for each House vote fetch members.
    Returns a structure suitable for attaching to a bill row (e.g. roll_call_votes_json).
    """
    if not api_key:
        return {"error": "API key required"}

    bill = fetch_bill(congress, bill_type, bill_number, api_key)
    if not bill:
        return {"error": "Bill not found", "congress": congress, "billType": bill_type, "billNumber": bill_number}

    actions = fetch_bill_actions(congress, bill_type, bill_number, api_key)
    recorded_votes = extract_recorded_votes_from_actions(actions)

    # Build list: metadata for every vote; for House votes add member-level data
    roll_calls = []
    for rv in recorded_votes:
        chamber = (rv.get("chamber") or "").strip()
        c = rv.get("congress")
        sess = rv.get("sessionNumber")
        roll = rv.get("rollNumber")
        entry = {
            "chamber": chamber,
            "congress": c,
            "sessionNumber": sess,
            "rollNumber": roll,
            "date": rv.get("date"),
            "url": rv.get("url"),
            "fullActionName": rv.get("fullActionName"),
            "memberVotes": None,
        }
        if chamber == "House" and c is not None and sess is not None and roll is not None:
            detail = fetch_house_vote_detail(int(c), int(sess), int(roll), api_key)
            if detail:
                entry["result"] = detail.get("result")
                entry["voteQuestion"] = detail.get("voteQuestion")
                entry["voteType"] = detail.get("voteType")
                entry["legislationUrl"] = detail.get("legislationUrl")
            members = fetch_house_vote_members(int(c), int(sess), int(roll), api_key)
            entry["memberVotes"] = members
            if debug_empty_members and len(members) == 0:
                url = f"{API_BASE_URL}/house-vote/{int(c)}/{int(sess)}/{int(roll)}/members"
                data = make_request(url, {**_params(api_key), "format": "json"})
                if data:
                    print(f"      [debug] house-vote members response keys: {list(data.keys()) if isinstance(data, dict) else 'list'}", file=sys.stderr)
                    raw = data.get("houseRollCallVoteMemberVotes") if isinstance(data, dict) else None
                    if raw is not None:
                        t = type(raw).__name__
                        if isinstance(raw, dict):
                            print(f"      [debug] houseRollCallVoteMemberVotes type=dict keys={list(raw.keys())}", file=sys.stderr)
                        else:
                            print(f"      [debug] houseRollCallVoteMemberVotes type={t} len={len(raw) if isinstance(raw, (list, dict)) else 'n/a'}", file=sys.stderr)
                else:
                    print(f"      [debug] house-vote members response: empty or error", file=sys.stderr)
                # List votes for this congress/session to verify vote exists in API
                session_votes = fetch_house_vote_list(int(c), int(sess), api_key)
                roll_nums = [v.get("rollCallNumber") or v.get("rollNumber") for v in session_votes if isinstance(v, dict)]
                print(f"      [debug] house-vote list {int(c)}/{int(sess)}: {len(session_votes)} vote(s), rollNumbers: {roll_nums[:15]}{'...' if len(roll_nums) > 15 else ''}", file=sys.stderr)
        roll_calls.append(entry)

    return {
        "bill_id": f"{congress}-{bill_type.upper()}-{bill_number}",
        "congress": congress,
        "billType": bill_type.upper(),
        "billNumber": str(bill_number),
        "billTitle": (bill.get("title") or "").strip() or (bill.get("titles", {}).get("title") or ""),
        "rollCallVotes": roll_calls,
        "rollCallCount": len(roll_calls),
    }


def format_vote_list_table(member_votes: List[Dict]) -> List[str]:
    """
    Format member votes as lines matching the official style:
    LastName, FirstName [Party-State]  PartyFull  StateFull  Vote
    Sorted by last name, then first name.
    """
    if not member_votes:
        return []
    party_full = lambda p: PARTY_TO_FULL.get((p or "").strip().upper(), (p or "").strip())
    state_full = lambda s: STATE_ABBR_TO_FULL.get((s or "").strip().upper(), (s or "").strip())
    rows = []
    for m in member_votes:
        last = (m.get("lastName") or "").strip()
        first = (m.get("firstName") or "").strip()
        party = (m.get("voteParty") or "").strip().upper() or "?"
        state = (m.get("voteState") or "").strip().upper() or "?"
        vote = (m.get("voteCast") or "").strip()
        name_part = f"{last}, {first} [{party}-{state}]"
        rows.append((name_part, party_full(party), state_full(state), vote))
    rows.sort(key=lambda r: (r[0].split(",")[0].strip().lower(), r[0]))
    return [f"{name}\t{party}\t{state}\t{vote}" for name, party, state, vote in rows]


def run_all_rolls(
    congress: int,
    session: Optional[int],
    api_key: str,
    debug: bool,
    output_path: Optional[str],
) -> None:
    """
    Fetch all roll calls for congress/session(s) using house-vote list API.
    Matches Glue second-pass logic so you can test locally before running on AWS.
    """
    sessions = [session] if session is not None else [1, 2]
    all_votes: List[Dict] = []
    for sess in sessions:
        vote_list = fetch_house_vote_list(congress, sess, api_key, debug_keys=debug)
        print(f"Congress {congress} Session {sess}: house-vote list API returned {len(vote_list)} roll(s).")
        for v in vote_list:
            v_copy = dict(v)
            v_copy.setdefault("sessionNumber", sess)
            all_votes.append(v_copy)
        if debug and len(vote_list) == 0:
            print("      [debug] No rolls parsed; response keys were logged above.", file=sys.stderr)
    roll_ids = [
        (v.get("rollCallNumber") or v.get("rollNumber"), v.get("sessionNumber"))
        for v in all_votes if isinstance(v, dict)
    ]
    print(f"Total roll calls: {len(all_votes)}")
    if all_votes:
        sample = roll_ids[:15]
        print(f"Sample (rollNumber, session): {sample}{'...' if len(roll_ids) > 15 else ''}")
        # Optionally fetch members for first roll to verify members API
        first = all_votes[0]
        rn = first.get("rollCallNumber") or first.get("rollNumber")
        sn = first.get("sessionNumber") or sessions[0]
        if rn is not None and sn is not None:
            members = fetch_house_vote_members(congress, int(sn), int(rn), api_key)
            print(f"First roll (Congress {congress} Session {sn} Roll {rn}): {len(members)} member votes")
    if output_path:
        out = {"congress": congress, "sessions": sessions, "rollCount": len(all_votes), "votes": all_votes}
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"Wrote {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch roll call (vote) data for a bill from Congress.gov API, or all roll calls for a congress/session."
    )
    parser.add_argument(
        "bill_number",
        type=str,
        nargs="?",
        default=None,
        help="Bill number (e.g. 2189 for H.R. 2189). Omit when using --all-rolls.",
    )
    parser.add_argument(
        "--congress",
        type=int,
        default=None,
        help="Congress number (default: current)",
    )
    parser.add_argument(
        "--type",
        dest="bill_type",
        type=str,
        default="hr",
        help="Bill type: hr, s, hjres, sjres, etc. (default: hr)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default="",
        help="Congress.gov API key (or set API_KEY in script)",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Write JSON to file",
    )
    parser.add_argument(
        "--votes-file",
        type=str,
        default=None,
        help="Write vote list (Name [Party-State], Party, State, Vote) to TSV file",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print API response keys when member list is empty or when --all-rolls returns 0",
    )
    parser.add_argument(
        "--all-rolls",
        action="store_true",
        help="Fetch all roll calls for congress/session (house-vote list API). No bill number needed.",
    )
    parser.add_argument(
        "--session",
        type=int,
        default=None,
        choices=[1, 2],
        help="Session (1 or 2). For --all-rolls only; if omitted, fetches both sessions.",
    )
    args = parser.parse_args()

    congress = args.congress or get_current_congress()
    api_key = (args.api_key or os.environ.get("CONGRESS_API_KEY") or API_KEY or "").strip()
    if not api_key:
        print("Set CONGRESS_API_KEY env var or pass --api-key (do not commit keys to the repo).", file=sys.stderr)
        sys.exit(1)

    if args.all_rolls:
        run_all_rolls(congress, args.session, api_key, args.debug, args.output)
        return

    if not args.bill_number:
        parser.error("bill_number required unless --all-rolls is used")
    print(f"Bill: Congress {congress}, {args.bill_type.upper()} {args.bill_number}")
    print("Fetching bill, actions, and roll call data...")
    result = build_roll_call_data_for_bill(
        congress, args.bill_type, args.bill_number, api_key,
        debug_empty_members=args.debug,
    )

    if result.get("error"):
        print(result["error"])
        sys.exit(2)

    print(f"Roll calls found: {result.get('rollCallCount', 0)}")
    all_vote_lines = []
    for rc in result.get("rollCallVotes", []):
        mv = rc.get("memberVotes") or []
        print(f"  - {rc.get('chamber')} roll {rc.get('rollNumber')} (session {rc.get('sessionNumber')}): {len(mv)} member votes")
        if mv:
            lines = format_vote_list_table(mv)
            all_vote_lines.extend(lines)

    if all_vote_lines:
        vote_list_text = "\n".join(all_vote_lines)
        if args.votes_file:
            with open(args.votes_file, "w", encoding="utf-8") as f:
                f.write(vote_list_text)
            print(f"Wrote vote list ({len(all_vote_lines)} members) to {args.votes_file}")
        else:
            print("\n--- Vote list (matching bill roll call) ---")
            print(vote_list_text)
            print("---")

    json_str = json.dumps(result, indent=2, default=str)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(json_str)
        print(f"Wrote {args.output}")
    else:
        print(json_str)


if __name__ == "__main__":
    main()
