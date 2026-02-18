"""
Roll call indexing for Congress bills: SEARCH#ROLL and SEARCH#VOTE.
Used by the fetcher after storing each bill. Set context via set_context() before calling.
"""
from __future__ import annotations

import json
import gzip
import re
import time
import threading
from collections import deque
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# Context set by fetcher: table, s3_client, bucket_name, api_base_url, get_api_key, log_print, request_timeout
_ctx: Optional[Dict[str, Any]] = None


class RollCallIndexError(Exception):
    """Raised when a roll call exists but the vote index (SEARCH#VOTE) was not created; job should fail."""
    pass

_BATCH_GET_MAX = 100
_BATCH_WRITE_MAX = 25
_OVERSIZE_SAFE_SIZE = int(400 * 1024 * 0.85)
_MAX_RETRIES = 5
_RETRY_DELAY = 2


def set_context(ctx: Dict[str, Any]) -> None:
    global _ctx
    _ctx = ctx


def _log(msg: str) -> None:
    if _ctx and callable(_ctx.get("log_print")):
        _ctx["log_print"](msg)


def _table():
    return _ctx and _ctx.get("table")


def _s3():
    return _ctx and _ctx.get("s3_client")


def _bucket():
    return _ctx and _ctx.get("bucket_name") or ""


def _api_base():
    return _ctx and _ctx.get("api_base_url") or "https://api.congress.gov/v3"


def _get_api_key():
    if _ctx and callable(_ctx.get("get_api_key")):
        return _ctx["get_api_key"]()
    return None, 0


def _request_timeout():
    return _ctx and _ctx.get("request_timeout") or 30


# Rate limiter (simple, per-call)
class _RateLimiter:
    def __init__(self, max_per_hour: int = 4800):
        self.max_per_hour = max_per_hour
        self._timestamps = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.time()
            while self._timestamps and self._timestamps[0] < now - 3600:
                self._timestamps.popleft()
            while len(self._timestamps) >= self.max_per_hour:
                wait = self._timestamps[0] + 3600 - time.time()
                if wait > 0:
                    time.sleep(wait)
                now = time.time()
                while self._timestamps and self._timestamps[0] < now - 3600:
                    self._timestamps.popleft()
            self._timestamps.append(time.time())


_rate_limiter = _RateLimiter()


def _make_api_request(url: str, params: Dict, api_key: str, key_index: Optional[int] = None) -> Optional[Dict]:
    _rate_limiter.acquire()
    if "api_key" not in params:
        params = {**params, "api_key": api_key}
    for attempt in range(_MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=_request_timeout())
            if r.status_code == 429:
                wait = max(5, (2 ** attempt) * _RETRY_DELAY)
                _log(f"      Rate limited (429) - waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < _MAX_RETRIES - 1:
                time.sleep(_RETRY_DELAY * (attempt + 1))
            else:
                _log(f"      Request failed: {str(e)[:100]}")
                return None
    return None


def _parse_bill_id(bill_id: str) -> Optional[Tuple[str, str, str]]:
    if not bill_id or bill_id.startswith("SEARCH#"):
        return None
    parts = bill_id.split("-")
    if len(parts) != 3:
        return None
    c, bt, bn = parts[0], parts[1], parts[2]
    if not c.isdigit() or not bn.isdigit():
        return None
    return (c, bt, bn)


def _house_rolls_from_recorded_votes_json(recorded_votes_json: Any) -> Set[Tuple[str, str]]:
    out: Set[Tuple[str, str]] = set()
    if not recorded_votes_json:
        return out
    raw = recorded_votes_json
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return out
    if not isinstance(raw, list):
        return out
    for v in raw:
        if not isinstance(v, dict):
            continue
        if (v.get("chamber") or "").strip().upper() != "HOUSE":
            continue
        s, r = v.get("sessionNumber"), v.get("rollNumber")
        if s is None or r is None:
            continue
        out.add((str(s), str(r)))
    return out


def _existing_rolls_from_roll_call_votes(roll_call_votes: Any) -> Set[Tuple[str, str]]:
    out: Set[Tuple[str, str]] = set()
    if not roll_call_votes:
        return out
    raw = roll_call_votes
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return out
    if not isinstance(raw, list):
        return out
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        s, r = entry.get("session"), entry.get("roll")
        if s is None or r is None:
            continue
        out.add((str(s), str(r)))
    return out


def _rolls_with_empty_members(roll_call_votes: Any) -> Set[Tuple[str, str]]:
    """Return (session, roll) for entries that have no members (need re-fetch to populate SEARCH#VOTE)."""
    out: Set[Tuple[str, str]] = set()
    if not roll_call_votes:
        return out
    raw = roll_call_votes
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return out
    if not isinstance(raw, list):
        return out
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        s, r = entry.get("session"), entry.get("roll")
        if s is None or r is None:
            continue
        members = entry.get("members")
        if not members or (isinstance(members, list) and len(members) == 0):
            out.add((str(s), str(r)))
    return out


def _member_vote_type(member: Dict) -> Optional[str]:
    """Vote value from member dict; Congress API v3 may use voteCast, vote_cast, vote, or value."""
    for key in ("voteCast", "vote_cast", "vote", "value"):
        v = member.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def _looks_like_member(obj: Any) -> bool:
    """True if obj looks like a member vote (has vote or bioguide)."""
    if not isinstance(obj, dict):
        return False
    for key in ("voteCast", "vote_cast", "vote", "value", "bioguideID", "bioguideId", "bioguide_id"):
        if obj.get(key) is not None:
            return True
    return False


def _normalize_member(member: Any) -> Optional[Dict]:
    """Return a plain dict for a member; unwrap DynamoDB-style {'M': {...}} or {'S'}/{'N'} values."""
    if not isinstance(member, dict):
        return None
    # Unwrap DynamoDB Map: {"M": {"firstName": {"S": "Jake"}, ...}}
    if "M" in member and isinstance(member["M"], dict):
        member = member["M"]
    out = {}
    for k, v in member.items():
        if isinstance(v, dict):
            if "S" in v:
                out[k] = str(v["S"])
            elif "N" in v:
                try:
                    out[k] = int(v["N"]) if "." not in str(v["N"]) else float(v["N"])
                except (ValueError, TypeError):
                    out[k] = v["N"]
            elif "M" in v:
                out[k] = _normalize_member(v["M"])  # nested map
            else:
                out[k] = v
        else:
            out[k] = v
    return out


def _extract_members_from_vote_response(data: Any) -> List[Dict]:
    """Extract list of member-vote dicts from Congress API v3 house-vote members response.
    API returns camelCase; may be top-level (results/memberVotes) or nested under a wrapper."""
    if not isinstance(data, dict):
        return []
    # Top-level keys (Congress API v3 may use these)
    for key in (
        "results",
        "members",
        "memberVotes",
        "houseRollCallVoteMembers",
        "houseRollCallVoteMemberVotes",
        "memberVoteList",
    ):
        val = data.get(key)
        if isinstance(val, list):
            if val and _looks_like_member(val[0]):
                return val
            # Unwrap: list of { "memberVote": {...} } or { "member": {...} }
            out = []
            for item in val:
                if _looks_like_member(item):
                    out.append(item)
                elif isinstance(item, dict):
                    for sub in ("memberVote", "member", "memberVoteDetails"):
                        if _looks_like_member(item.get(sub)):
                            out.append(item.get(sub))
                            break
            if out:
                return out
        if isinstance(val, dict):
            for sub in ("item", "memberVote", "memberVotes", "members", "results"):
                v = val.get(sub)
                if isinstance(v, list) and v and _looks_like_member(v[0]):
                    return v
    # Nested: houseRollCallVoteMemberVotes.item etc.
    raw = data.get("houseRollCallVoteMemberVotes")
    if isinstance(raw, list) and raw and _looks_like_member(raw[0]):
        return raw
    if isinstance(raw, dict):
        for k in ("item", "memberVote", "memberVotes", "houseRollCallVoteMemberVote"):
            v = raw.get(k)
            if isinstance(v, list) and v:
                return v
            if isinstance(v, dict) and _looks_like_member(v):
                return [v]
    # Fallback: find any list of member-like dicts one level deep
    for key, val in data.items():
        if isinstance(val, list) and len(val) > 0:
            first = val[0]
            if isinstance(first, dict) and _looks_like_member(first):
                return val
    return []


def _parse_vote_date(data: Dict) -> Optional[str]:
    for key in ("updateDate", "startDate", "date", "actionDate", "voteDate"):
        val = data.get(key)
        if not val:
            continue
        s = str(val).strip()
        if len(s) >= 10 and s[:10].replace("-", "").isdigit():
            return s[:10]
        if "T" in s:
            return s.split("T")[0][:10]
    return None


def _fetch_house_vote_members(
    congress: str, session: int, roll_number: int, api_key: str, key_index: Optional[int] = None
) -> Tuple[List[Dict], Optional[str]]:
    all_members = []
    vote_date = None
    offset = 0
    limit = 250
    base = _api_base()
    while True:
        url = f"{base}/house-vote/{congress}/{session}/{roll_number}/members"
        params = {"format": "json", "offset": offset, "limit": limit}
        data = _make_api_request(url, params, api_key, key_index)
        if not data:
            break
        if offset == 0 and isinstance(data, dict):
            vote_date = _parse_vote_date(data)
        results = _extract_members_from_vote_response(data)
        if results:
            all_members.extend(results)
        elif offset == 0 and isinstance(data, dict):
            # Log once per roll when API returned data but we parsed 0 members (helps debug response shape)
            top_keys = list(data.keys())[:12]
            _log(f"      House-vote members response had 0 members (top-level keys: {top_keys})")
        pagination = data.get("pagination") or {} if isinstance(data, dict) else {}
        count = pagination.get("count", 0) if isinstance(pagination, dict) else 0
        if count and offset + limit >= count:
            break
        if not (isinstance(results, list) and len(results) == limit):
            break
        offset += limit
    return all_members, vote_date


def _fetch_new_rolls_only(
    bill_id: str, to_fetch: Set[Tuple[str, str]], api_key: str, key_index: Optional[int] = None
) -> List[Dict]:
    parsed = _parse_bill_id(bill_id)
    if not parsed or not to_fetch:
        return []
    congress, _, _ = parsed
    result = []
    for session_s, roll_s in sorted(to_fetch):
        session = int(session_s) if session_s.isdigit() else 0
        roll = int(roll_s) if roll_s.isdigit() else 0
        if session <= 0 or roll <= 0:
            continue
        members, vote_date = _fetch_house_vote_members(congress, session, roll, api_key, key_index)
        entry = {"roll": roll, "session": session, "members": members}
        if vote_date:
            entry["vote_date"] = vote_date
        result.append(entry)
    return result


def _merge_roll_call_votes(
    existing_votes: List[Dict], xml_rolls: Set[Tuple[str, str]], new_rolls_data: List[Dict]
) -> List[Dict]:
    # Prefer new_rolls_data (has members from API) over existing entries (may have empty members)
    by_key: Dict[Tuple[str, str], Dict] = {}
    for entry in existing_votes or []:
        if not isinstance(entry, dict):
            continue
        s, r = entry.get("session"), entry.get("roll")
        if s is None or r is None:
            continue
        if (str(s), str(r)) in xml_rolls:
            by_key[(str(s), str(r))] = entry
    for e in new_rolls_data:
        s, r = e.get("session"), e.get("roll")
        if s is not None and r is not None:
            by_key[(str(s), str(r))] = e
    return [by_key[k] for k in sorted(by_key.keys())]


def _find_politician_by_bioguide(bioguide_id: str, politicians: List[Dict]) -> Optional[Dict]:
    if not bioguide_id or not politicians:
        return None
    bid = str(bioguide_id).strip().upper()
    for p in politicians:
        pb = p.get("bioguide_id")
        if pb and str(pb).strip().upper() == bid:
            return p
    return None


def _fuzzy_match_name(name: str, politician: Dict) -> float:
    name_norm = re.sub(r"^(?:rep\.|sen\.|representative|senator)\s+", "", name.lower().strip(), flags=re.IGNORECASE)
    name_norm = re.sub(r"\s*\[[^\]]+\]", "", name_norm).strip()
    pol_norm = (politician.get("name") or "").lower().strip()
    if name_norm == pol_norm:
        return 1.0
    for alt in politician.get("alternativeNames") or []:
        if name_norm == (alt or "").lower().strip():
            return 1.0
    return SequenceMatcher(None, name_norm, pol_norm).ratio()


def _find_matching_politician(
    name: str, politicians: List[Dict], first_name: str = "", last_name: str = "", party: str = "", state: str = ""
) -> Optional[Dict]:
    if not name or not politicians:
        return None
    first_n = first_name.strip().lower() if first_name else ""
    last_n = last_name.strip().lower() if last_name else ""
    best, best_score = None, 0.0
    for p in politicians:
        score = _fuzzy_match_name(name, p) * 0.5
        if first_n and (p.get("first_name") or "").strip().lower() == first_n:
            score += 0.2
        if last_n and (p.get("last_name") or "").strip().lower() == last_n:
            score += 0.2
        if score > best_score:
            best_score, best = score, p
    return best if best and best_score >= 0.6 else None


def _normalize_vote_pid(pid: str) -> str:
    """Use uppercase for bioguide-style IDs so SEARCH#VOTE#<pid> matches seed items from CSV."""
    if not pid or pid.startswith("NAME#"):
        return pid
    if pid.replace("-", "").isalnum() and len(pid) <= 20:
        return pid.upper()
    return pid


def _resolve_politician_id_and_name(
    member: Dict, politicians: List[Dict], politicians_by_bioguide: Optional[Dict[str, Dict]] = None
) -> Tuple[Optional[str], Optional[str]]:
    bioguide = (
        (member.get("bioguideID") or member.get("bioguideId") or member.get("bioguide_id") or "").strip()
    )
    first = (member.get("firstName") or member.get("first_name") or "").strip()
    last = (member.get("lastName") or member.get("last_name") or "").strip()
    name = (member.get("name") or "").strip() or f"{first} {last}".strip()
    party = (member.get("voteParty") or member.get("party") or "").strip()
    state = (member.get("voteState") or member.get("state") or "").strip()
    if bioguide:
        pol = (politicians_by_bioguide or {}).get(bioguide.upper()) or _find_politician_by_bioguide(bioguide, politicians)
        if pol:
            pid = pol.get("bioguide_id") or bioguide
            return (_normalize_vote_pid(pid), pol.get("name") or name or bioguide)
        return (_normalize_vote_pid(bioguide), name or bioguide)
    if name or first or last:
        pol = _find_matching_politician(name or f"{first} {last}", politicians, first_name=first, last_name=last, party=party, state=state)
        if pol:
            pid = pol.get("bioguide_id") or ("NAME#" + re.sub(r"[^A-Za-z0-9]", "_", (pol.get("name") or name).strip()))
            return (_normalize_vote_pid(pid) if not pid.startswith("NAME#") else pid, pol.get("name") or name)
        pid = "NAME#" + re.sub(r"[^A-Za-z0-9]", "_", (name or f"{first}_{last}").strip())
        return (pid, name or f"{first} {last}".strip())
    return (None, None)


def _store_vote_data_to_s3(pk: str, vote_entries: List[Dict]) -> str:
    s3_key = f"oversize/{pk.replace('#', '-')}.json.gz"
    payload = {"vote_entries": vote_entries}
    compressed = gzip.compress(json.dumps(payload, default=str).encode("utf-8"))
    s3 = _s3()
    if s3 and _bucket():
        s3.put_object(Bucket=_bucket(), Key=s3_key, Body=compressed, ContentType="application/json", ContentEncoding="gzip")
    return s3_key


def _legacy_8_lists_to_vote_entries(
    bill_yea: List[str], bill_nea: List[str], bill_present: List[str], bill_not_voting: List[str],
    roll_yea: List[str], roll_nea: List[str], roll_present: List[str], roll_not_voting: List[str],
) -> List[Dict]:
    out = []
    types = ("Yea", "Nay", "Present", "Not Voting")
    for i, (bill_list, roll_list) in enumerate([
        (bill_yea, roll_yea), (bill_nea, roll_nea), (bill_present, roll_present), (bill_not_voting, roll_not_voting)
    ]):
        for j, roll_id in enumerate(roll_list):
            bill_id = bill_list[j] if j < len(bill_list) else ""
            out.append({"bill_id": bill_id or "", "roll_id": roll_id, "vote_type": types[i]})
    return out


def _load_vote_entries_from_item(existing_item: Dict) -> List[Dict]:
    vote_entries = list(existing_item.get("vote_entries") or [])
    s3_key = (existing_item.get("vote_data_oversize_s3_key") or "").strip()
    if s3_key:
        s3 = _s3()
        if s3 and _bucket():
            try:
                resp = s3.get_object(Bucket=_bucket(), Key=s3_key)
                raw = gzip.decompress(resp["Body"].read())
                data = json.loads(raw.decode("utf-8"))
                if data.get("vote_entries") is not None:
                    return list(data["vote_entries"])
                vote_entries = _legacy_8_lists_to_vote_entries(
                    list(data.get("bill_yea") or data.get("yea") or []),
                    list(data.get("bill_nea") or data.get("nea") or []),
                    list(data.get("bill_present") or []),
                    list(data.get("bill_not_voting") or []),
                    list(data.get("roll_yea") or []),
                    list(data.get("roll_nea") or []),
                    list(data.get("roll_present") or []),
                    list(data.get("roll_not_voting") or []),
                )
            except Exception as e:
                _log(f"      Failed to load vote data from S3: {str(e)[:120]}")
                raise
    if vote_entries:
        return vote_entries
    def _list(k: str, leg: Optional[str] = None) -> List[str]:
        v = existing_item.get(k) or (existing_item.get(leg) if leg else [])
        return list(v or [])
    return _legacy_8_lists_to_vote_entries(
        _list("bill_yea", "yea"), _list("bill_nea", "nea"), _list("bill_present"), _list("bill_not_voting"),
        _list("roll_yea"), _list("roll_nea"), _list("roll_present"), _list("roll_not_voting"),
    )


def _merge_vote_entries_one_per_roll(entries: List[Dict]) -> List[Dict]:
    """Merge vote entries so there is exactly one per roll_id; prefer entry with non-empty bill_id."""
    by_roll: Dict[str, Dict] = {}
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        roll_id = (e.get("roll_id") or "").strip()
        if not roll_id:
            continue
        bill = (e.get("bill_id") or "").strip()
        if roll_id not in by_roll:
            by_roll[roll_id] = e
        else:
            existing_bill = (by_roll[roll_id].get("bill_id") or "").strip()
            if bill and not existing_bill:
                by_roll[roll_id] = e
    return [by_roll[k] for k in sorted(by_roll.keys())]


def _store_roll_call_votes_to_s3(bill_id: str, roll_call_votes: List[Dict]) -> str:
    s3_key = f"oversize/{bill_id}-roll_call_votes.json.gz"
    compressed = gzip.compress(json.dumps(roll_call_votes, default=str).encode("utf-8"))
    _s3().put_object(Bucket=_bucket(), Key=s3_key, Body=compressed, ContentType="application/json", ContentEncoding="gzip")
    return s3_key


def _store_roll_members_to_s3(congress: str, session: int, roll: int, members: List[Dict]) -> str:
    s3_key = f"oversize/SEARCH-ROLL-{congress}-{session}-{roll}.json.gz"
    compressed = gzip.compress(json.dumps(members, default=str).encode("utf-8"))
    _s3().put_object(Bucket=_bucket(), Key=s3_key, Body=compressed, ContentType="application/json", ContentEncoding="gzip")
    return s3_key


def update_search_vote_index_for_bill(
    bill_id: str,
    roll_call_votes: List[Dict],
    politicians: List[Dict],
    politicians_by_bioguide: Optional[Dict[str, Dict]] = None,
) -> None:
    table = _table()
    if not table:
        return
    parsed = _parse_bill_id(bill_id)
    congress = int(parsed[0]) if parsed and parsed[0].isdigit() else None
    updates: Dict[str, Dict] = {}
    for entry in roll_call_votes or []:
        if not isinstance(entry, dict):
            continue
        session_raw, roll_raw = entry.get("session"), entry.get("roll")
        sess_int = int(session_raw) if session_raw is not None and str(session_raw).isdigit() else (int(session_raw) if isinstance(session_raw, (int, float)) else None)
        roll_int = int(roll_raw) if roll_raw is not None and str(roll_raw).isdigit() else (int(roll_raw) if isinstance(roll_raw, (int, float)) else None)
        roll_id = f"{congress}#{sess_int}#{roll_int}" if (congress and sess_int is not None and roll_int is not None) else ""
        for member in entry.get("members") or []:
            member = _normalize_member(member) if member else None
            if not member:
                continue
            vote_type = _member_vote_type(member)
            if not vote_type:
                continue
            pid, display_name = _resolve_politician_id_and_name(member, politicians, politicians_by_bioguide)
            if not pid:
                continue
            if pid not in updates:
                updates[pid] = {"vote_entries": [], "display_name": display_name or ""}
            updates[pid]["vote_entries"].append({"bill_id": bill_id, "roll_id": roll_id, "vote_type": vote_type})
            if display_name and not updates[pid]["display_name"]:
                updates[pid]["display_name"] = display_name
    if not updates:
        _log(f"      SEARCH#VOTE: no members with vote/resolved pid for bill {bill_id} (roll_call_votes has {sum(len(e.get('members') or []) for e in (roll_call_votes or []))} members)")
        return
    client = table.meta.client
    table_name = table.name
    keys = [{"bill_id": f"SEARCH#VOTE#{pid}", "search_index_sk": "VOTE"} for pid in updates]
    existing: Dict[str, Dict] = {}
    for i in range(0, len(keys), _BATCH_GET_MAX):
        chunk = keys[i : i + _BATCH_GET_MAX]
        resp = client.batch_get_item(RequestItems={table_name: {"Keys": chunk}})
        for item in resp.get("Responses", {}).get(table_name, []):
            pk = item.get("bill_id") or ""
            if pk.startswith("SEARCH#VOTE#"):
                existing[pk.replace("SEARCH#VOTE#", "", 1)] = item
        unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        while unprocessed:
            time.sleep(0.2)
            resp = client.batch_get_item(RequestItems={table_name: {"Keys": unprocessed}})
            for item in resp.get("Responses", {}).get(table_name, []):
                pk = item.get("bill_id") or ""
                if pk.startswith("SEARCH#VOTE#"):
                    existing[pk.replace("SEARCH#VOTE#", "", 1)] = item
            unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
    put_items = []
    for pid, data in updates.items():
        item = existing.get(pid) or {}
        existing_entries = _load_vote_entries_from_item(item)
        combined = existing_entries + data["vote_entries"]
        vote_entries = _merge_vote_entries_one_per_roll(combined)
        display_name = (data.get("display_name") or item.get("display_name") or item.get("search_value") or "").strip() or pid
        full_item = {
            "bill_id": f"SEARCH#VOTE#{pid}",
            "search_index_sk": "VOTE",
            "search_type": "VOTE",
            "search_value": display_name,
            "display_name": display_name,
            "vote_entries": vote_entries,
            "is_search_index": True,
        }
        if len(json.dumps(full_item, default=str)) > _OVERSIZE_SAFE_SIZE:
            s3_key = _store_vote_data_to_s3(f"SEARCH#VOTE#{pid}", vote_entries)
            full_item = {
                "bill_id": f"SEARCH#VOTE#{pid}",
                "search_index_sk": "VOTE",
                "search_type": "VOTE",
                "search_value": display_name,
                "display_name": display_name,
                "vote_data_oversize_s3_key": s3_key,
                "is_search_index": True,
            }
        put_items.append(full_item)
    with table.batch_writer() as batch:
        for item in put_items:
            batch.put_item(Item=item)


def _write_one_roll_item(
    congress: str, session_int: int, roll_int: int, members: List[Dict],
    bill_id_associated: str, latest_action_date: Optional[str] = None,
) -> None:
    table = _table()
    if not table:
        return
    date_part = (latest_action_date or "").strip()[:10] if latest_action_date else "0000-00-00"
    if not (len(date_part) >= 10 and date_part.replace("-", "").isdigit()):
        date_part = "0000-00-00"
    sk = f"{congress}#{date_part}#{session_int}#{roll_int}"
    item = {
        "bill_id": "SEARCH#ROLL",
        "search_index_sk": sk,
        "search_type": "ROLL",
        "search_value": sk,
        "congress": int(congress),
        "session": session_int,
        "roll": roll_int,
        "bill_id_associated": bill_id_associated,
        "roll_display": f"Roll no. {roll_int}",
        "members": members,
        "is_search_index": True,
    }
    if date_part != "0000-00-00":
        item["latest_action_date"] = date_part
    if len(json.dumps(item, default=str)) > _OVERSIZE_SAFE_SIZE:
        s3_key = _store_roll_members_to_s3(congress, session_int, roll_int, members)
        item = {
            "bill_id": "SEARCH#ROLL",
            "search_index_sk": sk,
            "search_type": "ROLL",
            "search_value": sk,
            "congress": int(congress),
            "session": session_int,
            "roll": roll_int,
            "bill_id_associated": bill_id_associated,
            "roll_display": f"Roll no. {roll_int}",
            "members_oversize_s3_key": s3_key,
            "is_search_index": True,
        }
        if date_part != "0000-00-00":
            item["latest_action_date"] = date_part
    try:
        table.put_item(Item=item)
    except Exception as e:
        _log(f"      SEARCH#ROLL put failed: {str(e)[:150]}")


def update_search_roll_index_for_bill(
    bill_id: str,
    roll_call_votes: List[Dict],
    rolls_written: Optional[Set[Tuple[str, int, int]]] = None,
    roll_write_lock: Optional[threading.Lock] = None,
    politicians: Optional[List[Dict]] = None,
    politicians_by_bioguide: Optional[Dict[str, Dict]] = None,
) -> None:
    table = _table()
    if not table:
        return
    parsed = _parse_bill_id(bill_id)
    if not parsed:
        return
    congress, _, _ = parsed
    bill_latest_date = None
    try:
        resp = table.get_item(Key={"bill_id": bill_id, "search_index_sk": bill_id})
        item = resp.get("Item")
        if item:
            d = item.get("latest_action_date")
            if d is not None:
                bill_latest_date = str(d).strip()[:10]
    except Exception:
        pass
    for entry in roll_call_votes or []:
        if not isinstance(entry, dict):
            continue
        session, roll = entry.get("session"), entry.get("roll")
        members = entry.get("members") or []
        if session is None or roll is None:
            continue
        session_int = int(session) if isinstance(session, (int, float)) else (int(session) if str(session).isdigit() else None)
        roll_int = int(roll) if isinstance(roll, (int, float)) else (int(roll) if str(roll).isdigit() else None)
        if session_int is None or roll_int is None:
            continue
        key = (congress, session_int, roll_int)
        if rolls_written is not None:
            if roll_write_lock:
                with roll_write_lock:
                    if key in rolls_written:
                        continue
                    rolls_written.add(key)
            else:
                if key in rolls_written:
                    continue
                rolls_written.add(key)
        vote_date = entry.get("vote_date") or bill_latest_date
        # Get roll → write votes → write roll (no read of roll row)
        members_plain = [_normalize_member(m) for m in members if _normalize_member(m)]
        if members and not members_plain:
            msg = (
                f"Roll call found but vote index not created: congress={congress} session={session_int} roll={roll_int} "
                f"bill_id={bill_id}: 0 members after normalize (had {len(members)} raw members)"
            )
            _log(f"      [FAIL] {msg}")
            raise RollCallIndexError(msg)
        if members_plain:
            congress_int = int(congress) if isinstance(congress, str) and congress.isdigit() else int(congress) if isinstance(congress, (int, float)) else None
            if congress_int is not None:
                wrote = update_search_vote_index_for_roll(
                    congress_int, session_int, roll_int, members_plain,
                    politicians or [], politicians_by_bioguide or {},
                )
                if not wrote:
                    msg = (
                        f"Roll call found but vote index not created: congress={congress} session={session_int} roll={roll_int} "
                        f"bill_id={bill_id}: no SEARCH#VOTE items produced ({len(members_plain)} members)"
                    )
                    _log(f"      [FAIL] {msg}")
                    raise RollCallIndexError(msg)
        _write_one_roll_item(congress, session_int, roll_int, members, bill_id, latest_action_date=vote_date)


def ensure_search_vote_items_for_legislators(politicians: List[Dict]) -> None:
    table = _table()
    if not table or not politicians:
        return
    pids_with_names: List[Tuple[str, str]] = []
    for p in politicians:
        pid = (p.get("bioguide_id") or "").strip()
        if not pid:
            pid = "NAME#" + re.sub(r"[^A-Za-z0-9]", "_", (p.get("name") or "unknown").strip())
        else:
            pid = _normalize_vote_pid(pid)
        display_name = (p.get("name") or "").strip() or pid
        pids_with_names.append((pid, display_name))
    client = table.meta.client
    table_name = table.name
    keys = [{"bill_id": f"SEARCH#VOTE#{pid}", "search_index_sk": "VOTE"} for pid, _ in pids_with_names]
    existing_pids: Set[str] = set()
    for i in range(0, len(keys), _BATCH_GET_MAX):
        chunk = keys[i : i + _BATCH_GET_MAX]
        try:
            resp = client.batch_get_item(RequestItems={table_name: {"Keys": chunk}})
            for item in resp.get("Responses", {}).get(table_name, []):
                pk = (item.get("bill_id") or "").strip()
                if pk.startswith("SEARCH#VOTE#"):
                    existing_pids.add(pk.replace("SEARCH#VOTE#", "", 1))
            unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
            while unprocessed:
                time.sleep(0.2)
                resp = client.batch_get_item(RequestItems={table_name: {"Keys": unprocessed}})
                for item in resp.get("Responses", {}).get(table_name, []):
                    pk = (item.get("bill_id") or "").strip()
                    if pk.startswith("SEARCH#VOTE#"):
                        existing_pids.add(pk.replace("SEARCH#VOTE#", "", 1))
                unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        except Exception as e:
            _log(f"      SEARCH#VOTE seed batch get failed: {str(e)[:150]}")
            return
    to_create = [(pid, name) for pid, name in pids_with_names if pid not in existing_pids]
    if not to_create:
        return
    put_items = []
    for pid, display_name in to_create:
        put_items.append({
            "bill_id": f"SEARCH#VOTE#{pid}",
            "search_index_sk": "VOTE",
            "search_type": "VOTE",
            "search_value": display_name,
            "display_name": display_name,
            "vote_entries": [],
            "is_search_index": True,
        })
    try:
        with table.batch_writer() as batch:
            for item in put_items:
                batch.put_item(Item=item)
    except Exception as e:
        _log(f"      SEARCH#VOTE seed batch write failed: {str(e)[:150]}")
        return
    _log(f"   Seeded {len(to_create)} SEARCH#VOTE item(s) for legislators -> table={table.name}")


def run_roll_call_delta_for_bill(
    bill_id: str,
    record: Dict[str, Any],
    politicians: List[Dict],
    politicians_by_bioguide: Optional[Dict[str, Dict]] = None,
    rolls_written: Optional[Set[Tuple[str, int, int]]] = None,
    roll_write_lock: Optional[threading.Lock] = None,
) -> bool:
    """
    Compare recorded_votes_json in record to existing roll_call_votes; fetch only new rolls, merge, update bill, SEARCH#ROLL, SEARCH#VOTE.
    Returns True if bill was updated (roll call data changed).
    """
    table = _table()
    if not table:
        return False
    recorded_votes_json = record.get("recorded_votes_json")
    roll_call_votes_raw = record.get("roll_call_votes")
    xml_rolls = _house_rolls_from_recorded_votes_json(recorded_votes_json)
    existing_rolls = _existing_rolls_from_roll_call_votes(roll_call_votes_raw)
    rolls_missing_members = _rolls_with_empty_members(roll_call_votes_raw)
    if not xml_rolls:
        return False
    # Fetch rolls we don't have OR rolls we have but with empty members (so we can populate SEARCH#VOTE)
    to_fetch = (xml_rolls - existing_rolls) | rolls_missing_members
    if not to_fetch:
        return False
    api_key, key_index = _get_api_key()
    if not api_key:
        _log("      No API key for roll call fetch; skipping.")
        return False
    existing_list = roll_call_votes_raw
    if isinstance(existing_list, str):
        try:
            existing_list = json.loads(existing_list)
        except json.JSONDecodeError:
            existing_list = []
    if not isinstance(existing_list, list):
        existing_list = []
    try:
        new_rolls_data = _fetch_new_rolls_only(bill_id, to_fetch, api_key, key_index)
    except Exception as e:
        _log(f"      Roll call fetch failed for {bill_id}: {str(e)[:150]}")
        return False
    merged = _merge_roll_call_votes(existing_list, xml_rolls, new_rolls_data)
    roll_call_number = merged[0].get("roll") if merged else None
    has_roll_call = 1 if merged else 0
    try:
        if has_roll_call:
            update_expr = "SET has_roll_call = :h, roll_call_number = :n, roll_call_votes = :v REMOVE roll_call_votes_oversize_s3_key"
            attr_vals = {":h": 1, ":n": roll_call_number, ":v": json.dumps(merged)}
        else:
            update_expr = "SET has_roll_call = :h REMOVE roll_call_number, roll_call_votes, roll_call_votes_oversize_s3_key"
            attr_vals = {":h": 0}
        table.update_item(
            Key={"bill_id": bill_id, "search_index_sk": record.get("search_index_sk", bill_id)},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=attr_vals,
        )
    except Exception as e:
        err_str = str(e)
        if "ValidationException" in err_str and "exceeded" in err_str.lower() and merged:
            s3_key = _store_roll_call_votes_to_s3(bill_id, merged)
            table.update_item(
                Key={"bill_id": bill_id, "search_index_sk": record.get("search_index_sk", bill_id)},
                UpdateExpression="SET has_roll_call = :h, roll_call_number = :n, roll_call_votes_oversize_s3_key = :k REMOVE roll_call_votes",
                ExpressionAttributeValues={":h": 1, ":n": roll_call_number, ":k": s3_key},
            )
        else:
            raise
    if merged and politicians:
        update_search_vote_index_for_bill(bill_id, merged, politicians, politicians_by_bioguide)
    if merged:
        update_search_roll_index_for_bill(
            bill_id, merged,
            rolls_written=rolls_written, roll_write_lock=roll_write_lock,
            politicians=politicians, politicians_by_bioguide=politicians_by_bioguide,
        )
    return True


def _load_existing_roll_keys_for_congress(congress: int) -> Set[Tuple[str, int, int]]:
    from boto3.dynamodb.conditions import Key
    table = _table()
    if not table:
        return set()
    out: Set[Tuple[str, int, int]] = set()
    pk, sk_prefix = "SEARCH#ROLL", f"{congress}#"
    try:
        pagination = {}
        while True:
            resp = table.query(
                KeyConditionExpression=Key("bill_id").eq(pk) & Key("search_index_sk").begins_with(sk_prefix),
                ProjectionExpression="search_index_sk, #s, #r",
                ExpressionAttributeNames={"#s": "session", "#r": "roll"},
                **pagination,
            )
            for item in resp.get("Items") or []:
                session_val = item.get("session")
                roll_val = item.get("roll")
                if session_val is not None and roll_val is not None:
                    out.add((str(congress), int(session_val) if not isinstance(session_val, int) else session_val, int(roll_val) if not isinstance(roll_val, int) else roll_val))
            next_key = resp.get("LastEvaluatedKey")
            if not next_key:
                break
            pagination = {"ExclusiveStartKey": next_key}
    except Exception as e:
        _log(f"      Query SEARCH#ROLL for congress {congress}: {str(e)[:200]}")
    return out


def _bill_id_from_house_vote_item(v: Dict) -> str:
    """Build bill_id from Congress API v3 house-vote list item (legislationType, legislationNumber, congress)."""
    if not isinstance(v, dict):
        return ""
    congress = v.get("congress")
    leg_type = (v.get("legislationType") or "").strip().upper()
    leg_num = (v.get("legislationNumber") or "").strip()
    if congress is not None and leg_type and leg_num:
        return f"{congress}-{leg_type}-{leg_num}"
    return ""


def _fetch_house_vote_list(congress: int, session: int, api_key: str, key_index: Optional[int] = None) -> List[Dict]:
    """Fetch all house-vote list items for congress/session; API uses offset/limit (max 250) pagination."""
    out = []
    offset, limit = 0, 250
    base = _api_base()
    while True:
        url = f"{base}/house-vote/{congress}/{session}"
        params = {"format": "json", "offset": offset, "limit": limit}
        data = _make_api_request(url, params, api_key, key_index)
        if not data:
            break
        items = None
        if isinstance(data, list):
            items = data
        if items is None:
            items = data.get("votes") or data.get("houseVotes") or data.get("results") or data.get("items")
        if isinstance(items, dict):
            items = items.get("item", items.get("vote", []))
        if not isinstance(items, list):
            # Single vote object returned (e.g. limit=1 or API quirk)
            if isinstance(data, dict) and data.get("rollCallNumber") is not None:
                items = [data]
            else:
                items = []
        if items:
            out.extend(items)
        if len(items) < limit:
            break
        offset += limit
    return out


def update_search_vote_index_for_roll(
    congress: int, session: int, roll: int, members: List[Dict],
    politicians: List[Dict], politicians_by_bioguide: Optional[Dict[str, Dict]] = None,
    bill_id_override: Optional[str] = None,
) -> bool:
    roll_id = f"{congress}#{session}#{roll}"
    bill_for_entry = (bill_id_override or "").strip()
    updates: Dict[str, Dict] = {}
    for member in members or []:
        member = _normalize_member(member) if member else None
        if not member:
            continue
        vote_type = _member_vote_type(member)
        if not vote_type:
            continue
        pid, display_name = _resolve_politician_id_and_name(member, politicians, politicians_by_bioguide)
        if not pid:
            continue
        if pid not in updates:
            updates[pid] = {"vote_entries": [], "display_name": display_name or ""}
        updates[pid]["vote_entries"].append({"bill_id": bill_for_entry, "roll_id": roll_id, "vote_type": vote_type})
        if display_name and not updates[pid]["display_name"]:
            updates[pid]["display_name"] = display_name
    if not updates:
        _log(f"      SEARCH#VOTE: no pids/votes for roll {congress}/{session}/{roll} ({len(members or [])} members)")
        return False
    table = _table()
    if not table:
        return False
    client = table.meta.client
    table_name = table.name
    keys = [{"bill_id": f"SEARCH#VOTE#{pid}", "search_index_sk": "VOTE"} for pid in updates]
    existing = {}
    for i in range(0, len(keys), _BATCH_GET_MAX):
        chunk = keys[i : i + _BATCH_GET_MAX]
        resp = client.batch_get_item(RequestItems={table_name: {"Keys": chunk}})
        for item in resp.get("Responses", {}).get(table_name, []):
            pk = item.get("bill_id") or ""
            if pk.startswith("SEARCH#VOTE#"):
                existing[pk.replace("SEARCH#VOTE#", "", 1)] = item
        unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        while unprocessed:
            time.sleep(0.2)
            resp = client.batch_get_item(RequestItems={table_name: {"Keys": unprocessed}})
            for item in resp.get("Responses", {}).get(table_name, []):
                pk = item.get("bill_id") or ""
                if pk.startswith("SEARCH#VOTE#"):
                    existing[pk.replace("SEARCH#VOTE#", "", 1)] = item
            unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
    put_items = []
    for pid, data in updates.items():
        item = existing.get(pid) or {}
        existing_entries = _load_vote_entries_from_item(item)
        combined = existing_entries + data["vote_entries"]
        vote_entries = _merge_vote_entries_one_per_roll(combined)
        display_name = (data.get("display_name") or item.get("display_name") or item.get("search_value") or "").strip() or pid
        full_item = {
            "bill_id": f"SEARCH#VOTE#{pid}",
            "search_index_sk": "VOTE",
            "search_type": "VOTE",
            "search_value": display_name,
            "display_name": display_name,
            "vote_entries": vote_entries,
            "is_search_index": True,
        }
        if len(json.dumps(full_item, default=str)) > _OVERSIZE_SAFE_SIZE:
            s3_key = _store_vote_data_to_s3(f"SEARCH#VOTE#{pid}", vote_entries)
            full_item = {
                "bill_id": f"SEARCH#VOTE#{pid}",
                "search_index_sk": "VOTE",
                "search_type": "VOTE",
                "search_value": display_name,
                "display_name": display_name,
                "vote_data_oversize_s3_key": s3_key,
                "is_search_index": True,
            }
        put_items.append(full_item)
    try:
        # Use Table resource batch_writer (same serialization as table.put_item / SEARCH#ROLL)
        # so items are written correctly; low-level client.batch_write_item expects AttributeValue format.
        with table.batch_writer() as batch:
            for item in put_items:
                batch.put_item(Item=item)
        # Each row has PK bill_id="SEARCH#VOTE#<bioguide>", SK search_index_sk="VOTE" (no single "SEARCH#VOTE" row)
        sample_pks = [it["bill_id"] for it in put_items[:2]]
        _log(f"      SEARCH#VOTE: wrote {len(put_items)} items for roll {congress}/{session}/{roll} -> table={table.name} (e.g. query bill_id={sample_pks[0] if sample_pks else 'N/A'} search_index_sk=VOTE)")
        return True
    except Exception as e:
        msg = f"SEARCH#VOTE: batch write failed for roll {congress}/{session}/{roll}: {e}"
        _log(f"      {msg}")
        raise RollCallIndexError(msg) from e


def run_house_vote_second_pass(
    congresses: Set[int],
    politicians: List[Dict],
    politicians_by_bioguide: Optional[Dict[str, Dict]] = None,
    rolls_written_from_bills: Optional[Set[Tuple[str, int, int]]] = None,
) -> None:
    """Fetch house-vote list for each congress/session; for rolls not already in SEARCH#ROLL, fetch members and write SEARCH#ROLL + SEARCH#VOTE."""
    if not congresses:
        return
    _log("Second pass: house-vote list — fetch additional roll calls not already written from bills.")
    for congress in sorted(congresses):
        existing_roll_keys = _load_existing_roll_keys_for_congress(congress)
        _log(f"   Congress {congress}: {len(existing_roll_keys)} roll(s) already in SEARCH#ROLL.")
        for session in (1, 2):
            api_key, key_index = _get_api_key()
            if not api_key:
                continue
            vote_list = _fetch_house_vote_list(congress, session, api_key, key_index)
            _log(f"   Congress {congress} Session {session}: house-vote list returned {len(vote_list)} roll(s).")
            for v in vote_list:
                if not isinstance(v, dict):
                    continue
                roll_num = v.get("rollCallNumber") or v.get("rollNumber")
                sess_num = v.get("sessionNumber") or session
                if roll_num is None:
                    continue
                sess_int = int(sess_num) if not isinstance(sess_num, int) else sess_num
                roll_int = int(roll_num) if not isinstance(roll_num, int) else roll_num
                key = (str(congress), sess_int, roll_int)
                if key in existing_roll_keys:
                    continue
                if rolls_written_from_bills and (str(congress), sess_int, roll_int) in rolls_written_from_bills:
                    continue
                bill_id_associated = _bill_id_from_house_vote_item(v)
                latest_action_date = (v.get("startDate") or v.get("updateDate") or "").strip()[:10] or None
                api_key2, key_index2 = _get_api_key()
                if not api_key2:
                    continue
                members, _ = _fetch_house_vote_members(str(congress), sess_int, roll_int, api_key2, key_index2)
                # Get roll → write votes → write roll; fail job if roll has members but vote index not created
                members_plain = [_normalize_member(m) for m in members if _normalize_member(m)]
                if members and not members_plain:
                    msg = (
                        f"Roll call found but vote index not created: congress={congress} session={sess_int} roll={roll_int} "
                        f"(second pass): 0 members after normalize (had {len(members)} raw members)"
                    )
                    _log(f"      [FAIL] {msg}")
                    raise RollCallIndexError(msg)
                if members_plain:
                    wrote = update_search_vote_index_for_roll(
                        congress, sess_int, roll_int, members_plain,
                        politicians or [], politicians_by_bioguide or {},
                        bill_id_override=bill_id_associated or None,
                    )
                    if not wrote:
                        msg = (
                            f"Roll call found but vote index not created: congress={congress} session={sess_int} roll={roll_int} "
                            f"(second pass): no SEARCH#VOTE items produced ({len(members_plain)} members)"
                        )
                        _log(f"      [FAIL] {msg}")
                        raise RollCallIndexError(msg)
                _write_one_roll_item(
                    str(congress), sess_int, roll_int, members,
                    bill_id_associated or "", latest_action_date=latest_action_date,
                )
                existing_roll_keys.add(key)
