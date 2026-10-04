#!/usr/bin/env python3
"""
Interactive openFEC test — comprehensive candidate & committee money profiles.

Usage:
  python test_fec_politician_search.py
  # or: python3.13 test_fec_politician_search.py

Stdlib only (no pip install). Set FEC_API_KEY below or export FEC_API_KEY.
Uses api.open.fec.gov (same as ApiDocs.txt).

Cycle: all profile API calls use one FEC two-year cycle (default = derived from
today's date; override with FEC_CYCLE env or --cycle 2024). ApiDocs: in odd years
the current cycle is the next even year (e.g. May 2025 → cycle 2026).

Batch / Glue indexing (not one-at-a-time search only):
  - LIST endpoints paginate: /candidates/, /committees/, /elections/, /filings/
  - FILTER by cycle, state, office, is_active_candidate, committee_type
  - Per-entity detail uses the same calls as this script (see profile_api_manifest())
  - Production v1: nightly full re-index only (no cache, no intraday pulls). Align job after
    FEC’s nightly load; include schedule_e/efile in that single pass for fresher IE when filed.
  - Optional bulk export: POST /v1/download/{path} (async CSV, up to 500k rows per query)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Union
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Configuration — paste your key here
# ---------------------------------------------------------------------------
FEC_API_KEY = "PASTE_YOUR_API_KEY_HERE"

API_BASE = "https://api.open.fec.gov/v1"
REQUEST_TIMEOUT = 90
REQUEST_RETRIES = 2
RATE_LIMIT_DELAY = 0.35
SEARCH_PER_PAGE = 25
# Per-page size for cycle-scoped pulls (openFEC max 100 per page; Glue paginates fully)
CYCLE_PER_PAGE = 100
# Shorter samples where we only show a preview label in the CLI
PREVIEW_LIMIT = 10
# Limit extra race / PAC drill-down calls (openFEC can be slow on Schedule E)
RACE_RIVAL_IE_LIMIT = 1
TOP_PAC_FUNDRAISER_LIMIT = 2
PAC_DONOR_SAMPLE = 5


@dataclass
class SearchHit:
    kind: str  # "candidate" | "committee"
    entity_id: str
    name: str
    subtitle: str
    cycles: Optional[List[int]] = None


def _api_key() -> str:
    key = (os.environ.get("FEC_API_KEY") or FEC_API_KEY or "").strip()
    if not key or key == "PASTE_YOUR_API_KEY_HERE":
        print("Set FEC_API_KEY in this file or export FEC_API_KEY in your environment.")
        sys.exit(1)
    return key


def _normalize_name(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9\s]", "", value)
    return re.sub(r"\s+", " ", value).strip()


def _money(amount: Any) -> str:
    if amount is None:
        return "—"
    try:
        return f"${float(amount):,.2f}"
    except (TypeError, ValueError):
        return str(amount)


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            return True
        return "timed out" in str(exc).lower()
    return False


def fec_cycle_for_date(on_date: Optional[date] = None) -> int:
    """
    FEC cycle is named for the ending even year (2025–2026 activity → cycle 2026).
    In odd-numbered years the 'current' cycle is the next year (ApiDocs).
    """
    d = on_date or date.today()
    y = d.year
    return y if y % 2 == 0 else y + 1


def resolve_target_cycle(cli_cycle: Optional[int] = None) -> tuple[int, str]:
    if cli_cycle is not None:
        return int(cli_cycle), "--cycle CLI"
    env = (os.environ.get("FEC_CYCLE") or "").strip()
    if env:
        return int(env), "FEC_CYCLE env"
    today = date.today()
    return fec_cycle_for_date(today), f"today ({today.isoformat()})"


def _entity_has_cycle(entity_cycles: List[int], cycle: int) -> bool:
    return cycle in [int(c) for c in entity_cycles]


def _cycle_hint(cycles: List[int], target_cycle: int) -> str:
    if not cycles:
        return "cycles unknown"
    if target_cycle in cycles:
        return f"active in {target_cycle}"
    return f"cycles {min(cycles)}–{max(cycles)} (not {target_cycle})"


def _sort_hits_for_cycle(hits: List[SearchHit], cycle: int) -> List[SearchHit]:
    def sort_key(hit: SearchHit) -> tuple:
        if hit.cycles and cycle in hit.cycles:
            return (0, -max(hit.cycles))
        if hit.cycles:
            return (1, -max(hit.cycles))
        return (2, 0)

    return sorted(hits, key=sort_key)


def _format_district_ie(amount: Any, office: str) -> str:
    """openFEC /elections/summary/ IE can be wildly inflated for busy primaries."""
    try:
        val = float(amount)
    except (TypeError, ValueError):
        return _money(amount)
    cap = 50_000_000 if office == "house" else 500_000_000
    if val > cap:
        return "unreliable API aggregate — use per-candidate IE rows below"
    return _money(val)


class OpenFECClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[int] = None,
        allow_404: bool = False,
    ) -> Dict[str, Any]:
        query = dict(params or {})
        query["api_key"] = self.api_key
        url = f"{API_BASE}{path}?{urlencode(query, doseq=True)}"
        time.sleep(RATE_LIMIT_DELAY)
        req = Request(
            url,
            headers={"Accept": "application/json", "User-Agent": "Cosine-openFEC-test/1.0"},
        )
        wait = timeout if timeout is not None else REQUEST_TIMEOUT
        last_timeout: Optional[BaseException] = None
        for attempt in range(REQUEST_RETRIES + 1):
            try:
                with urlopen(req, timeout=wait) as resp:
                    body = resp.read().decode("utf-8")
                return json.loads(body)
            except HTTPError as exc:
                if allow_404 and exc.code == 404:
                    return {"results": [], "pagination": {"count": 0}}
                detail = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
                raise HTTPError(
                    exc.url, exc.code, f"{exc.reason}\n{detail[:500]}", exc.headers, None
                ) from exc
            except (TimeoutError, URLError) as exc:
                if not _is_timeout(exc):
                    raise
                last_timeout = exc
                if attempt < REQUEST_RETRIES:
                    time.sleep(1.5 * (attempt + 1))
        assert last_timeout is not None
        raise last_timeout

    def search_candidates(self, query: str, cycle: int) -> List[SearchHit]:
        hits: List[SearchHit] = []
        seen: set[str] = set()

        # Lightweight name lookup (good for partial / autocomplete-style queries)
        try:
            data = self.get("/names/candidates/", {"q": query})
            for row in data.get("results") or []:
                cid = row.get("id") or ""
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                hits.append(
                    SearchHit(
                        kind="candidate",
                        entity_id=cid,
                        name=row.get("name") or cid,
                        subtitle=f"Office sought: {row.get('office_sought') or '—'}",
                    )
                )
        except HTTPError:
            pass

        # Richer candidate search (party, state, cycles, principal committees)
        data = self.get(
            "/candidates/search/",
            {
                "q": query,
                "cycle": cycle,
                "per_page": SEARCH_PER_PAGE,
                "sort": "name",
            },
        )
        for row in data.get("results") or []:
            cid = row.get("candidate_id") or ""
            if not cid or cid in seen:
                continue
            seen.add(cid)
            office = row.get("office_full") or row.get("office") or "—"
            state = row.get("state") or "—"
            party = row.get("party_full") or row.get("party") or "—"
            cycles = [int(x) for x in (row.get("cycles") or [])]
            cycle_hint = _cycle_hint(cycles, cycle)
            hits.append(
                SearchHit(
                    kind="candidate",
                    entity_id=cid,
                    name=row.get("name") or cid,
                    subtitle=f"{office} · {state} · {party} · {cycle_hint}",
                    cycles=cycles or None,
                )
            )
        return hits

    def search_committees(self, query: str, cycle: int) -> List[SearchHit]:
        hits: List[SearchHit] = []
        seen: set[str] = set()

        try:
            data = self.get("/names/committees/", {"q": query})
            for row in data.get("results") or []:
                cid = row.get("id") or ""
                if not cid or cid in seen:
                    continue
                seen.add(cid)
                active = "active" if row.get("is_active") else "inactive"
                hits.append(
                    SearchHit(
                        kind="committee",
                        entity_id=cid,
                        name=row.get("name") or cid,
                        subtitle=f"Committee ({active})",
                    )
                )
        except HTTPError:
            pass

        data = self.get(
            "/committees/",
            {
                "q": query,
                "cycle": cycle,
                "per_page": SEARCH_PER_PAGE,
                "sort": "name",
            },
        )
        for row in data.get("results") or []:
            cid = row.get("committee_id") or ""
            if not cid or cid in seen:
                continue
            seen.add(cid)
            ctype = row.get("committee_type_full") or row.get("committee_type") or "—"
            state = row.get("state") or "—"
            cycles = [int(x) for x in (row.get("cycles") or [])]
            hits.append(
                SearchHit(
                    kind="committee",
                    entity_id=cid,
                    name=row.get("name") or cid,
                    subtitle=f"{ctype} · {state} · {_cycle_hint(cycles, cycle)}",
                    cycles=cycles or None,
                )
            )
        return hits

    def candidate_detail(self, candidate_id: str) -> Dict[str, Any]:
        return self.get(f"/candidate/{candidate_id}/")

    def candidate_totals(self, candidate_id: str, cycle: int) -> List[Dict[str, Any]]:
        data = self.get(
            f"/candidate/{candidate_id}/totals/",
            {"cycle": cycle, "per_page": 20, "sort": "-cycle"},
            allow_404=True,
        )
        return data.get("results") or []

    def candidate_committees(
        self, candidate_id: str, cycle: int, per_page: int = 50
    ) -> List[Dict[str, Any]]:
        data = self.get(
            f"/candidate/{candidate_id}/committees/",
            {"per_page": per_page, "cycle": cycle},
        )
        return data.get("results") or []

    def candidate_filings(
        self, candidate_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            f"/candidate/{candidate_id}/filings/",
            {
                "cycle": cycle,
                "per_page": per_page,
                "sort": "-receipt_date",
            },
        )
        return data.get("results") or []

    def committee_detail(self, committee_id: str) -> Dict[str, Any]:
        return self.get(f"/committee/{committee_id}/")

    def committee_totals(self, committee_id: str, cycle: int) -> List[Dict[str, Any]]:
        data = self.get(
            f"/committee/{committee_id}/totals/",
            {"cycle": cycle, "per_page": 20, "sort": "-cycle"},
            allow_404=True,
        )
        return data.get("results") or []

    def committee_filings(
        self, committee_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            f"/committee/{committee_id}/filings/",
            {
                "cycle": cycle,
                "per_page": per_page,
                "sort": "-receipt_date",
            },
        )
        return data.get("results") or []

    def schedule_a(
        self, committee_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_a/",
            {
                "committee_id": committee_id,
                "two_year_transaction_period": cycle,
                "per_page": per_page,
                "sort": "-contribution_receipt_date",
            },
        )
        return data.get("results") or []

    def schedule_b(
        self, committee_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_b/",
            {
                "committee_id": committee_id,
                "two_year_transaction_period": cycle,
                "per_page": per_page,
                "sort": "-disbursement_date",
            },
        )
        return data.get("results") or []

    def _totals_by_candidate(
        self, path: str, candidate_id: str, cycle: int
    ) -> List[Dict[str, Any]]:
        data = self.get(
            path,
            {
                "candidate_id": candidate_id,
                "cycle": cycle,
                "election_full": "true",
                "per_page": 20,
            },
        )
        return data.get("results") or []

    def ie_totals_by_candidate(self, candidate_id: str, cycle: int) -> List[Dict[str, Any]]:
        return self._totals_by_candidate(
            "/schedules/schedule_e/totals/by_candidate/", candidate_id, cycle
        )

    def electioneering_totals_by_candidate(
        self, candidate_id: str, cycle: int
    ) -> List[Dict[str, Any]]:
        return self._totals_by_candidate(
            "/electioneering/totals/by_candidate/", candidate_id, cycle
        )

    def communication_costs_totals_by_candidate(
        self, candidate_id: str, cycle: int
    ) -> List[Dict[str, Any]]:
        return self._totals_by_candidate(
            "/communication_costs/totals/by_candidate/", candidate_id, cycle
        )

    def ie_spenders_by_candidate(
        self, candidate_id: str, cycle: int, per_page: int = 20
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_e/by_candidate/",
            {
                "candidate_id": candidate_id,
                "cycle": cycle,
                "election_full": "true",
                "per_page": per_page,
            },
        )
        return data.get("results") or []

    def recent_independent_expenditures(
        self,
        candidate_id: str,
        cycle: int,
        per_page: int = CYCLE_PER_PAGE,
        *,
        timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_e/",
            {
                "candidate_id": candidate_id,
                "two_year_transaction_period": cycle,
                "per_page": per_page,
                "sort": "-expenditure_date",
            },
            timeout=timeout,
        )
        return data.get("results") or []

    def recent_independent_expenditures_efile(
        self,
        candidate_id: str,
        per_page: int = CYCLE_PER_PAGE,
        *,
        timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_e/efile/",
            {
                "candidate_id": candidate_id,
                "per_page": per_page,
                "sort": "-expenditure_date",
            },
            timeout=timeout,
        )
        return data.get("results") or []

    def electioneering_by_candidate(
        self, candidate_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/electioneering/by_candidate/",
            {
                "candidate_id": candidate_id,
                "cycle": cycle,
                "election_full": "true",
                "per_page": per_page,
            },
        )
        return data.get("results") or []

    def communication_costs_by_candidate(
        self, candidate_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/communication_costs/by_candidate/",
            {
                "candidate_id": candidate_id,
                "cycle": cycle,
                "election_full": "true",
                "per_page": per_page,
            },
        )
        return data.get("results") or []

    def party_coordinated_expenditures(
        self, candidate_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_f/",
            {
                "candidate_id": candidate_id,
                "two_year_transaction_period": cycle,
                "per_page": per_page,
                "sort": "-expenditure_date",
            },
        )
        return data.get("results") or []

    def elections_in_district(
        self, cycle: int, office: str, state: str, district: str, per_page: int = 20
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/elections/",
            {
                "cycle": cycle,
                "office": office,
                "state": state,
                "district": district,
                "election_full": "true",
                "per_page": per_page,
                "sort": "-total_receipts",
            },
        )
        return data.get("results") or []

    def elections_summary(
        self, cycle: int, office: str, state: str, district: str
    ) -> Dict[str, Any]:
        data = self.get(
            "/elections/summary/",
            {
                "cycle": cycle,
                "office": office,
                "state": state,
                "district": district,
                "election_full": "true",
            },
        )
        if isinstance(data, dict) and "results" in data:
            results = data.get("results") or []
            return results[0] if results else {}
        return data if isinstance(data, dict) else {}

    def committee_linked_candidates(
        self, committee_id: str, cycle: int, per_page: int = 20
    ) -> List[Dict[str, Any]]:
        data = self.get(
            f"/committee/{committee_id}/candidates/",
            {"cycle": cycle, "per_page": per_page},
        )
        return data.get("results") or []

    def committee_reports(
        self, committee_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            f"/committee/{committee_id}/reports/",
            {
                "cycle": cycle,
                "per_page": per_page,
                "is_amended": "false",
                "sort": "-coverage_end_date",
            },
            allow_404=True,
        )
        return data.get("results") or []

    def schedule_a_by_size(
        self, committee_id: str, cycle: int, per_page: int = 20
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_a/by_size/",
            {
                "committee_id": committee_id,
                "two_year_transaction_period": cycle,
                "per_page": per_page,
            },
        )
        return data.get("results") or []

    def schedule_a_by_size_for_candidate(
        self, candidate_id: str, cycle: int, per_page: int = 20
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_a/by_size/by_candidate/",
            {
                "candidate_id": candidate_id,
                "cycle": cycle,
                "election_full": "true",
                "per_page": per_page,
            },
        )
        return data.get("results") or []

    def schedule_a_by_state_totals_for_candidate(
        self, candidate_id: str, cycle: int, per_page: int = 20
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_a/by_state/by_candidate/totals/",
            {
                "candidate_id": candidate_id,
                "cycle": cycle,
                "election_full": "true",
                "per_page": per_page,
            },
        )
        return data.get("results") or []

    def schedule_b_by_recipient(
        self, committee_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_b/by_recipient/",
            {
                "committee_id": committee_id,
                "cycle": cycle,
                "per_page": per_page,
                "sort": "-total",
            },
        )
        return data.get("results") or []

    def schedule_b_by_purpose(
        self, committee_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_b/by_purpose/",
            {
                "committee_id": committee_id,
                "cycle": cycle,
                "per_page": per_page,
                "sort": "-total",
            },
        )
        return data.get("results") or []

    def schedule_e_for_committee(
        self,
        committee_id: str,
        cycle: int,
        per_page: int = CYCLE_PER_PAGE,
        *,
        timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_e/",
            {
                "committee_id": committee_id,
                "two_year_transaction_period": cycle,
                "per_page": per_page,
                "sort": "-expenditure_date",
            },
            timeout=timeout,
        )
        return data.get("results") or []

    def schedule_e_by_candidate_for_committee(
        self, committee_id: str, cycle: int, per_page: int = CYCLE_PER_PAGE
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_e/by_candidate/",
            {
                "committee_id": committee_id,
                "cycle": cycle,
                "election_full": "true",
                "per_page": per_page,
            },
        )
        return data.get("results") or []

    def schedule_e_efile_for_committee(
        self,
        committee_id: str,
        per_page: int = CYCLE_PER_PAGE,
        *,
        timeout: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        data = self.get(
            "/schedules/schedule_e/efile/",
            {
                "committee_id": committee_id,
                "per_page": per_page,
                "sort": "-expenditure_date",
            },
            timeout=timeout,
        )
        return data.get("results") or []

    def list_filings_global(
        self,
        *,
        min_receipt_date: Optional[str] = None,
        committee_id: Optional[str] = None,
        per_page: int = 1,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "per_page": per_page,
            "sort": "-receipt_date",
            "most_recent": "true",
        }
        if min_receipt_date:
            params["min_receipt_date"] = min_receipt_date
        if committee_id:
            params["committee_id"] = committee_id
        data = self.get("/filings/", params)
        return data.get("results") or []


def profile_api_manifest(entity_kind: str) -> List[str]:
    """Endpoint paths used for a full profile — map 1:1 to Glue indexer tasks."""
    common = [
        "GET /{entity}/{id}/",
        "GET /{entity}/{id}/totals/?cycle=",
        "GET /{entity}/{id}/filings/",
        "GET /schedules/schedule_a/?committee_id=",
        "GET /schedules/schedule_b/?committee_id=",
        "GET /{entity}/{id}/reports/?cycle=",
        "GET /schedules/schedule_a/by_size/?committee_id=",
    ]
    if entity_kind == "candidate":
        return common + [
            "GET /candidate/{id}/committees/",
            "GET /elections/summary/",
            "GET /elections/",
            "GET /schedules/schedule_e/totals/by_candidate/",
            "GET /schedules/schedule_e/by_candidate/?candidate_id=",
            "GET /schedules/schedule_e/?candidate_id=",
            "GET /schedules/schedule_e/efile/?candidate_id=",
            "GET /electioneering/totals/by_candidate/",
            "GET /communication_costs/totals/by_candidate/",
            "GET /schedules/schedule_f/?candidate_id=",
            "GET /schedules/schedule_a/by_size/by_candidate/",
            "GET /schedules/schedule_a/by_state/by_candidate/totals/",
        ]
    return common + [
        "GET /committee/{id}/candidates/?cycle=",
        "GET /schedules/schedule_b/by_recipient/?committee_id=",
        "GET /schedules/schedule_b/by_purpose/?committee_id=",
        "GET /schedules/schedule_e/?committee_id=  (super PAC)",
        "GET /schedules/schedule_e/by_candidate/?committee_id=  (super PAC)",
        "GET /schedules/schedule_e/efile/?committee_id=  (super PAC)",
    ]


def _office_api_name(office_code: Optional[str]) -> Optional[str]:
    mapping = {"H": "house", "S": "senate", "P": "president"}
    if not office_code:
        return None
    return mapping.get(str(office_code).upper())


def _district_api_value(district: Any) -> str:
    if district is None or district == "":
        return "00"
    try:
        return f"{int(district):02d}"
    except (TypeError, ValueError):
        text = str(district).strip()
        return text if len(text) == 2 else text.zfill(2)


def _exact_match(
    query: str, hits: List[SearchHit], cycle: int
) -> Optional[SearchHit]:
    q = _normalize_name(query)
    if not q:
        return None
    matches = [h for h in hits if _normalize_name(h.name) == q]
    if matches:
        active = [h for h in matches if h.cycles and cycle in h.cycles]
        if len(active) == 1:
            return active[0]
        if len(matches) == 1:
            return matches[0]
        return None
    # Single strong match: full query appears as whole name
    if len(hits) == 1 and q in _normalize_name(hits[0].name):
        only = hits[0]
        if only.cycles and cycle not in only.cycles:
            return None
        return only
    return None


def _print_hits(hits: List[SearchHit]) -> None:
    if not hits:
        print("  No matches.")
        return
    for i, hit in enumerate(hits, 1):
        print(f"  [{i}] {hit.kind.upper():9} {hit.name}")
        print(f"       ID: {hit.entity_id}")
        print(f"       {hit.subtitle}")


RETYPE = object()


def _prompt_choice(hits: List[SearchHit]) -> Union[SearchHit, object, None]:
    print("\nCommands: enter a number to select, [r] retype search, [q] quit")
    while True:
        choice = input("> ").strip().lower()
        if choice in ("q", "quit", "exit"):
            return None
        if choice in ("r", "retry", "retype"):
            return RETYPE
        if not choice.isdigit():
            print("  Enter a list number, r, or q.")
            continue
        idx = int(choice)
        if 1 <= idx <= len(hits):
            return hits[idx - 1]
        print(f"  Choose 1–{len(hits)}.")


def _committee_is_super_pac(comm: Dict[str, Any]) -> bool:
    return (comm.get("committee_type") or "").upper() == "O"


def _print_totals_block(
    totals: List[Dict[str, Any]],
    cycle: int,
    *,
    entity_kind: str = "candidate",
    committee_meta: Optional[Dict[str, Any]] = None,
) -> None:
    if not totals:
        print(f"  No totals for cycle {cycle}.")
        return
    row = next((t for t in totals if t.get("cycle") == cycle), totals[0])
    if entity_kind == "committee":
        print(
            f"\n  Committee finances (Form 3/3X summary, cycle {row.get('cycle', cycle)}):"
        )
    else:
        print(
            f"\n  Authorized committee finances (Form 3/3X summary, cycle {row.get('cycle', cycle)}):"
        )
        print(
            "    (Candidate-controlled committee only — excludes super PAC / outside spenders.)"
        )
    print(f"    Receipts:              {_money(row.get('receipts'))}")
    print(f"    Disbursements:           {_money(row.get('disbursements'))}")
    print(f"    Cash on hand (end):      {_money(row.get('last_cash_on_hand_end_period'))}")
    print(f"    Individual contributions:{_money(row.get('individual_contributions'))}")
    print(f"    PAC contributions:       {_money(row.get('other_political_committee_contributions'))}")
    print(f"    Transfers from other:    {_money(row.get('transfers_from_other_authorized_committee'))}")
    print(f"    Debts owed:              {_money(row.get('last_debts_owed_by_committee'))}")
    if committee_meta and _committee_is_super_pac(committee_meta):
        print(f"    Independent expenditures:{_money(row.get('independent_expenditures'))}")
        print(
            f"    Total IE (reported):     {_money(row.get('total_independent_expenditures'))}"
        )


def _print_filings(filings: List[Dict[str, Any]], label: str, cycle: int) -> None:
    print(
        f"\n  Filings ({label}, cycle {cycle}, up to {CYCLE_PER_PAGE} per page — "
        "paginate in Glue for all):"
    )
    if not filings:
        print("    (none)")
        return
    for f in filings[:PREVIEW_LIMIT]:
        form = f.get("form_type") or "—"
        rdate = f.get("receipt_date") or f.get("coverage_end_date") or "—"
        cid = f.get("committee_id") or "—"
        print(f"    {rdate}  {form}  committee={cid}")
    if len(filings) > PREVIEW_LIMIT:
        print(f"    … showing {PREVIEW_LIMIT} of {len(filings)} returned on this page")


def _print_schedule_a(rows: List[Dict[str, Any]], cycle: int) -> None:
    print(f"\n  Itemized receipts (Schedule A, cycle {cycle}, page size {CYCLE_PER_PAGE}):")
    if not rows:
        print("    (none)")
        return
    for r in rows[:PREVIEW_LIMIT]:
        donor = r.get("contributor_name") or "—"
        amt = _money(r.get("contribution_receipt_amount"))
        dt = r.get("contribution_receipt_date") or "—"
        employer = r.get("contributor_employer") or ""
        extra = f" ({employer})" if employer else ""
        print(f"    {dt}  {amt}  {donor}{extra}")
    if len(rows) > PREVIEW_LIMIT:
        print(f"    … showing {PREVIEW_LIMIT} of {len(rows)} on this page")


def _print_reports(rows: List[Dict[str, Any]], label: str, cycle: int) -> None:
    print(
        f"\n  Financial reports ({label}, cycle {cycle}, "
        f"up to {CYCLE_PER_PAGE} per page):"
    )
    if not rows:
        print("    (none)")
        return
    for r in rows:
        rtype = r.get("report_type_full") or r.get("report_type") or "—"
        end = r.get("coverage_end_date") or "—"
        print(
            f"    {end}  {rtype}  receipts {_money(r.get('receipts'))}  "
            f"cash {_money(r.get('cash_on_hand_end_period'))}"
        )


def _print_donor_aggregates(
    by_size: List[Dict[str, Any]], by_state: List[Dict[str, Any]], label: str
) -> None:
    if by_size:
        print(f"\n  Contributions by size ({label}):")
        for row in by_size[:PREVIEW_LIMIT]:
            print(
                f"    size {row.get('size', '—')}: {_money(row.get('total'))} "
                f"({row.get('count', 0)} contributions)"
            )
    if by_state:
        print(f"\n  Contributions by state ({label}, top {PREVIEW_LIMIT}):")
        for row in by_state[:PREVIEW_LIMIT]:
            print(
                f"    {row.get('state') or '—'}: {_money(row.get('total'))} "
                f"({row.get('count', 0)} contributions)"
            )


def _print_schedule_b(rows: List[Dict[str, Any]], cycle: int) -> None:
    print(f"\n  Disbursements (Schedule B, cycle {cycle}, page size {CYCLE_PER_PAGE}):")
    if not rows:
        print("    (none)")
        return
    for r in rows[:PREVIEW_LIMIT]:
        recip = r.get("recipient_name") or r.get("payee_name") or "—"
        amt = _money(r.get("disbursement_amount"))
        dt = r.get("disbursement_date") or "—"
        purpose = r.get("disbursement_description") or ""
        extra = f" — {purpose}" if purpose else ""
        print(f"    {dt}  {amt}  {recip}{extra}")
    if len(rows) > PREVIEW_LIMIT:
        print(f"    … showing {PREVIEW_LIMIT} of {len(rows)} on this page")


_SUPPORT_OPPOSE = {"S": "support", "O": "oppose"}


def _print_race_context_block(
    client: OpenFECClient,
    candidate: Dict[str, Any],
    candidate_id: str,
    cycle: int,
) -> None:
    office = _office_api_name(candidate.get("office"))
    state = (candidate.get("state") or "").upper()
    district = _district_api_value(candidate.get("district"))
    if not office or not state:
        return

    label = f"{office} {state}-{district}" if office == "house" else f"{office} {state}"
    print(f"\n  Race context ({label}, cycle {cycle}):")
    summary = client.elections_summary(cycle, office, state, district)
    if summary:
        print(f"    Candidates filing:     {summary.get('count', '—')}")
        print(f"    All committee receipts:{_money(summary.get('receipts'))}")
        print(f"    All committee spending:{_money(summary.get('disbursements'))}")
        print(
            f"    District IE (aggregate):{_format_district_ie(summary.get('independent_expenditures'), office)}"
        )
    else:
        print("    No elections/summary row for this seat.")

    racers = client.elections_in_district(cycle, office, state, district)
    if not racers:
        print("    No per-candidate rows from /elections/.")
        return

    rivals = sorted(
        [r for r in racers if r.get("candidate_id") != candidate_id],
        key=lambda r: float(r.get("total_receipts") or 0),
        reverse=True,
    )
    rival_ie_ids = {
        r.get("candidate_id")
        for r in rivals[:RACE_RIVAL_IE_LIMIT]
        if r.get("candidate_id")
    }

    print(f"\n  Candidates in this seat ({len(racers)}):")
    for row in racers:
        cid = row.get("candidate_id") or "—"
        name = row.get("candidate_name") or "—"
        marker = " ←" if cid == candidate_id else ""
        print(
            f"    {name} ({cid}){marker}"
            f"  receipts {_money(row.get('total_receipts'))}"
            f"  disbursements {_money(row.get('total_disbursements'))}"
        )
        if cid in rival_ie_ids:
            try:
                for ie in client.ie_totals_by_candidate(cid, cycle):
                    ind = _SUPPORT_OPPOSE.get(ie.get("support_oppose_indicator") or "?", "?")
                    print(f"      IE ({ind}): {_money(ie.get('total'))}")
            except (TimeoutError, URLError) as exc:
                if _is_timeout(exc):
                    print(f"      IE: (skipped — timed out after {REQUEST_TIMEOUT}s)")
                else:
                    raise
    if len(rivals) > RACE_RIVAL_IE_LIMIT:
        print(
            f"    … IE totals omitted for {len(rivals) - RACE_RIVAL_IE_LIMIT} "
            f"other candidate(s) to limit API load"
        )


def _print_top_spender_committees(
    client: OpenFECClient, spenders: List[Dict[str, Any]], cycle: int
) -> None:
    top = sorted(spenders, key=lambda r: float(r.get("total") or 0), reverse=True)[
        :TOP_PAC_FUNDRAISER_LIMIT
    ]
    if not top:
        return
    print(f"\n  Top outside committees — fundraising (cycle {cycle}):")
    seen: set[str] = set()
    for row in top:
        cid = row.get("committee_id")
        if not cid or cid in seen:
            continue
        seen.add(cid)
        name = row.get("committee_name") or cid
        ind = _SUPPORT_OPPOSE.get(row.get("support_oppose_indicator") or "?", "?")
        try:
            totals = client.committee_totals(cid, cycle)
            trow = totals[0] if totals else {}
            print(
                f"    {name} ({cid})  IE {ind} {_money(row.get('total'))}  "
                f"PAC receipts {_money(trow.get('receipts'))}  "
                f"PAC disbursements {_money(trow.get('disbursements'))}"
            )
            donors = client.schedule_a(cid, cycle, per_page=PAC_DONOR_SAMPLE)
            if donors:
                print(f"      Recent PAC contributions ({PAC_DONOR_SAMPLE}):")
                for d in donors[:PAC_DONOR_SAMPLE]:
                    print(
                        f"        {_money(d.get('contribution_receipt_amount'))}  "
                        f"{d.get('contributor_name') or '—'}"
                    )
        except (TimeoutError, URLError) as exc:
            if _is_timeout(exc):
                print(f"    {name} ({cid})  (PAC detail skipped — timed out)")
            else:
                raise


def _print_outside_spending_block(
    client: OpenFECClient, candidate_id: str, cycle: int
) -> None:
    print("\n  Outside spending targeting this candidate (FEC, not committee receipts):")
    print(
        "    News and FEC race pages often cite district-wide primary spending (all "
        "candidates + for/against). That is not the same as this candidate's official "
        "receipts or as money 'given to' their committee."
    )

    ie = client.ie_totals_by_candidate(candidate_id, cycle)
    ec = client.electioneering_totals_by_candidate(candidate_id, cycle)
    cc = client.communication_costs_totals_by_candidate(candidate_id, cycle)

    if not ie and not ec and not cc:
        print("    No outside-spending aggregates on file for this cycle.")
    else:
        for row in ie:
            ind = row.get("support_oppose_indicator") or "?"
            label = _SUPPORT_OPPOSE.get(ind, ind)
            print(
                f"    Independent expenditures ({label}): "
                f"{_money(row.get('total'))}"
            )
        for row in ec:
            print(f"    Electioneering communications: {_money(row.get('total'))}")
        for row in cc:
            ind = row.get("support_oppose_indicator") or "?"
            label = _SUPPORT_OPPOSE.get(ind, ind)
            print(
                f"    Corporate/union communication costs ({label}): "
                f"{_money(row.get('total'))}"
            )

    print(
        "\n    IE totals above exclude 24-/48-hour independent expenditure notices "
        "(openFEC aggregates by design). Recent line items below may include them."
    )

    spenders = client.ie_spenders_by_candidate(candidate_id, cycle)
    if spenders:
        print(f"\n  Outside spenders (Schedule E aggregated by committee, cycle {cycle}):")
        for row in sorted(
            spenders, key=lambda r: float(r.get("total") or 0), reverse=True
        ):
            ind = row.get("support_oppose_indicator") or "?"
            label = _SUPPORT_OPPOSE.get(ind, ind)
            comm = row.get("committee_name") or row.get("committee_id") or "—"
            print(
                f"    {comm}  {label}: {_money(row.get('total'))} "
                f"({row.get('count', 0)} filings)"
            )
        _print_top_spender_committees(client, spenders, cycle)

    if ec:
        try:
            ec_rows = client.electioneering_by_candidate(candidate_id, cycle)
            if ec_rows:
                print(f"\n  Electioneering by committee (preview {PREVIEW_LIMIT}):")
                for row in ec_rows[:PREVIEW_LIMIT]:
                    comm = row.get("committee_name") or row.get("committee_id") or "—"
                    print(f"    {comm}  total {_money(row.get('total'))}")
        except (TimeoutError, URLError) as exc:
            if _is_timeout(exc):
                print("\n  Electioneering by committee: (skipped — timed out)")

    if cc:
        try:
            cc_rows = client.communication_costs_by_candidate(candidate_id, cycle)
            if cc_rows:
                print(f"\n  Communication costs by committee (preview {PREVIEW_LIMIT}):")
                for row in cc_rows[:PREVIEW_LIMIT]:
                    comm = row.get("committee_name") or row.get("committee_id") or "—"
                    ind = _SUPPORT_OPPOSE.get(
                        row.get("support_oppose_indicator") or "?", "?"
                    )
                    print(f"    {comm}  {ind}  total {_money(row.get('total'))}")
        except (TimeoutError, URLError) as exc:
            if _is_timeout(exc):
                print("\n  Communication costs by committee: (skipped — timed out)")

    try:
        sched_f = client.party_coordinated_expenditures(candidate_id, cycle)
        if sched_f:
            print(f"\n  Party coordinated expenditures (Schedule F, preview {PREVIEW_LIMIT}):")
            for row in sched_f[:PREVIEW_LIMIT]:
                comm = row.get("committee_name") or row.get("committee_id") or "—"
                amt = _money(
                    row.get("expenditure_amount")
                    or row.get("expenditure_amount_to_date")
                )
                dt = row.get("expenditure_date") or "—"
                print(f"    {dt}  {amt}  {comm}")
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("\n  Party coordinated (Schedule F): (skipped — timed out)")

    print(
        f"\n  Recent independent expenditures (Schedule E, cycle {cycle}, "
        f"page {CYCLE_PER_PAGE}, preview {PREVIEW_LIMIT}):"
    )
    try:
        recent = client.recent_independent_expenditures(
            candidate_id, cycle, timeout=60
        )
        if not recent:
            print("    (none)")
        else:
            for r in recent[:PREVIEW_LIMIT]:
                comm = (r.get("committee") or {}).get("name") or r.get("committee_id") or "—"
                amt = _money(r.get("expenditure_amount"))
                dt = r.get("expenditure_date") or "—"
                ind = _SUPPORT_OPPOSE.get(r.get("support_oppose_indicator") or "?", "?")
                desc = r.get("expenditure_description") or ""
                extra = f" — {desc}" if desc else ""
                print(f"    {dt}  {amt}  {ind}  {comm}{extra}")
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("    (skipped — Schedule E list timed out; aggregates above are still valid)")

    print(
        f"\n  Recent IE e-filings (schedule_e/efile, last ~4 months, preview {PREVIEW_LIMIT}):"
    )
    try:
        efile = client.recent_independent_expenditures_efile(
            candidate_id, timeout=45
        )
        if not efile:
            print("    (none)")
        else:
            for r in efile[:PREVIEW_LIMIT]:
                comm = (r.get("committee") or {}).get("name") or r.get("committee_id") or "—"
                amt = _money(r.get("expenditure_amount"))
                dt = r.get("expenditure_date") or r.get("dissemination_date") or "—"
                ind = _SUPPORT_OPPOSE.get(r.get("support_oppose_indicator") or "?", "?")
                print(f"    {dt}  {amt}  {ind}  {comm}")
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("    (skipped — efile endpoint timed out)")


def _print_candidate_donor_enrichment(
    client: OpenFECClient, candidate_id: str, cycle: int, principal_id: Optional[str]
) -> None:
    try:
        by_cand = client.schedule_a_by_size_for_candidate(candidate_id, cycle)
        by_state = client.schedule_a_by_state_totals_for_candidate(candidate_id, cycle)
        _print_donor_aggregates(by_cand, by_state, "all linked committees")
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("\n  Donor aggregates (by candidate): (skipped — timed out)")
    if principal_id:
        try:
            by_size = client.schedule_a_by_size(principal_id, cycle)
            _print_donor_aggregates(by_size, [], f"principal committee {principal_id}")
        except (TimeoutError, URLError) as exc:
            if _is_timeout(exc):
                print("\n  Donor aggregates (principal): (skipped — timed out)")


def _print_committee_enrichment(
    client: OpenFECClient, comm: Dict[str, Any], committee_id: str, cycle: int
) -> None:
    try:
        linked = client.committee_linked_candidates(committee_id, cycle)
        if linked:
            print(f"\n  Linked candidates ({len(linked)}):")
            for row in linked[:PREVIEW_LIMIT]:
                print(
                    f"    {row.get('candidate_name') or '—'} ({row.get('candidate_id')})  "
                    f"{row.get('office_full') or row.get('office') or ''} "
                    f"{row.get('state') or ''}"
                )
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("\n  Linked candidates: (skipped — timed out)")

    try:
        reports = client.committee_reports(committee_id, cycle)
        _print_reports(reports, "committee", cycle)
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("\n  Committee reports: (skipped — timed out)")

    try:
        by_size = client.schedule_a_by_size(committee_id, cycle)
        _print_donor_aggregates(by_size, [], "committee")
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("\n  Donor by size: (skipped — timed out)")

    try:
        recipients = client.schedule_b_by_recipient(committee_id, cycle)
        if recipients:
            print(f"\n  Disbursements by recipient (aggregated, preview {PREVIEW_LIMIT}):")
            for row in recipients[:PREVIEW_LIMIT]:
                print(
                    f"    {row.get('recipient_name') or '—'}: {_money(row.get('total'))} "
                    f"({row.get('count', 0)} payments)"
                )
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("\n  Schedule B by recipient: (skipped — timed out)")

    try:
        purposes = client.schedule_b_by_purpose(committee_id, cycle)
        if purposes:
            print(f"\n  Disbursements by purpose (preview {PREVIEW_LIMIT}):")
            for row in purposes[:PREVIEW_LIMIT]:
                print(
                    f"    {row.get('purpose') or row.get('disbursement_purpose_category') or '—'}: "
                    f"{_money(row.get('total'))}"
                )
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("\n  Schedule B by purpose: (skipped — timed out)")

    if not _committee_is_super_pac(comm):
        return

    print("\n  Super PAC independent expenditures:")
    try:
        for row in client.schedule_e_by_candidate_for_committee(committee_id, cycle):
            ind = _SUPPORT_OPPOSE.get(row.get("support_oppose_indicator") or "?", "?")
            print(
                f"    vs {row.get('candidate_name') or row.get('candidate_id')}: "
                f"{ind} {_money(row.get('total'))} ({row.get('count', 0)} filings)"
            )
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("    (by_candidate aggregate skipped — timed out)")

    print(
        f"\n  Recent IE (Schedule E, cycle {cycle}, preview {PREVIEW_LIMIT}):"
    )
    try:
        rows = client.schedule_e_for_committee(committee_id, cycle, timeout=60)
        for r in rows[:PREVIEW_LIMIT]:
            ind = _SUPPORT_OPPOSE.get(r.get("support_oppose_indicator") or "?", "?")
            cand = r.get("candidate_name") or r.get("candidate_id") or "—"
            print(
                f"    {r.get('expenditure_date') or '—'}  "
                f"{_money(r.get('expenditure_amount'))}  {ind}  {cand}"
            )
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("    (skipped — timed out)")

    print(f"\n  Recent IE e-filings (preview {PREVIEW_LIMIT}):")
    try:
        rows = client.schedule_e_efile_for_committee(committee_id, timeout=45)
        for r in rows[:PREVIEW_LIMIT]:
            ind = _SUPPORT_OPPOSE.get(r.get("support_oppose_indicator") or "?", "?")
            print(
                f"    {r.get('expenditure_date') or '—'}  "
                f"{_money(r.get('expenditure_amount'))}  {ind}"
            )
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            print("    (skipped — timed out)")


def show_candidate_profile(
    client: OpenFECClient, hit: SearchHit, cycle: int, cycle_source: str
) -> None:
    print("\n" + "=" * 72)
    print(f"CANDIDATE PROFILE: {hit.name}")
    print("=" * 72)
    detail = client.candidate_detail(hit.entity_id)
    results = detail.get("results") or []
    if not results:
        print("  Could not load candidate detail.")
        return
    c = results[0]
    cycles = [int(x) for x in (c.get("cycles") or [])]
    print(f"  ID:      {c.get('candidate_id')}")
    print(f"  Office:  {c.get('office_full')} ({c.get('state')})")
    print(f"  Party:   {c.get('party_full')}")
    print(f"  Status:  {c.get('candidate_status')} / {c.get('incumbent_challenge_full')}")
    print(f"  Cycles on file: {cycles}")
    if cycles and not _entity_has_cycle(cycles, cycle):
        print(
            f"  Warning: cycle {cycle} not in candidate cycles — API may return empty sections."
        )

    print(f"\n  Target cycle: {cycle} (from {cycle_source})")
    print("\n  Glue indexer manifest (candidate):")
    for line in profile_api_manifest("candidate"):
        print(f"    {line}")

    totals = client.candidate_totals(hit.entity_id, cycle)
    _print_totals_block(totals, cycle, entity_kind="candidate")
    _print_race_context_block(client, c, hit.entity_id, cycle)
    _print_outside_spending_block(client, hit.entity_id, cycle)

    committees = client.candidate_committees(hit.entity_id, cycle)
    print(f"\n  Linked committees ({len(committees)}):")
    for comm in committees[:12]:
        print(
            f"    {comm.get('committee_id')}  {comm.get('name')}  "
            f"({comm.get('committee_type_full') or comm.get('committee_type')})"
        )
    if len(committees) > 12:
        print(f"    … and {len(committees) - 12} more")

    filings = client.candidate_filings(hit.entity_id, cycle)
    _print_filings(filings, "candidate", cycle)

    principal = next(
        (x for x in committees if (x.get("designation") == "P")),
        committees[0] if committees else None,
    )
    pc_id = principal.get("committee_id") if principal else None
    _print_candidate_donor_enrichment(client, hit.entity_id, cycle, pc_id)

    if principal:
        print(
            f"\n  Principal committee {pc_id} ({principal.get('name')}) — "
            f"schedules (cycle {cycle}, page size {CYCLE_PER_PAGE}):"
        )
        try:
            reports = client.committee_reports(pc_id, cycle)
            _print_reports(reports, "principal committee", cycle)
        except (TimeoutError, URLError) as exc:
            if _is_timeout(exc):
                print("  Principal reports: (skipped — timed out)")
        _print_schedule_a(client.schedule_a(pc_id, cycle), cycle)
        _print_schedule_b(client.schedule_b(pc_id, cycle), cycle)


def show_committee_profile(
    client: OpenFECClient, hit: SearchHit, cycle: int, cycle_source: str
) -> None:
    print("\n" + "=" * 72)
    print(f"COMMITTEE PROFILE: {hit.name}")
    print("=" * 72)
    detail = client.committee_detail(hit.entity_id)
    results = detail.get("results") or []
    if not results:
        print("  Could not load committee detail.")
        return
    comm = results[0]
    cycles = [int(x) for x in (comm.get("cycles") or [])]
    print(f"  ID:           {comm.get('committee_id')}")
    print(f"  Type:         {comm.get('committee_type_full')}")
    print(f"  Designation:  {comm.get('designation_full')}")
    print(f"  Treasurer:    {comm.get('treasurer_name')}")
    print(f"  State:        {comm.get('state')}")
    print(f"  Cycles on file: {cycles}")
    if cycles and not _entity_has_cycle(cycles, cycle):
        print(
            f"  Warning: cycle {cycle} not in committee cycles — totals/reports may be empty."
        )
        print(
            "  Tip: search for the candidate (e.g. MASSIE, THOMAS H.) to get the "
            f"cycle {cycle} principal committee, not an old namesake committee."
        )

    print(f"\n  Target cycle: {cycle} (from {cycle_source})")
    print("\n  Glue indexer manifest (committee):")
    for line in profile_api_manifest("committee"):
        print(f"    {line}")

    totals = client.committee_totals(hit.entity_id, cycle)
    _print_totals_block(
        totals, cycle, entity_kind="committee", committee_meta=comm
    )

    filings = client.committee_filings(hit.entity_id, cycle)
    _print_filings(filings, "committee", cycle)
    if filings:
        latest = filings[0].get("receipt_date") or filings[0].get("coverage_end_date")
        print(f"    Latest filing on file: {latest}")

    _print_committee_enrichment(client, comm, hit.entity_id, cycle)

    _print_schedule_a(client.schedule_a(hit.entity_id, cycle), cycle)
    _print_schedule_b(client.schedule_b(hit.entity_id, cycle), cycle)


def run_search_flow(
    client: OpenFECClient, query: str, cycle: int, cycle_source: str
) -> None:
    print(f"\nSearching openFEC for: {query!r} (cycle {cycle})")
    candidates = client.search_candidates(query, cycle)
    committees = client.search_committees(query, cycle)
    all_hits = _sort_hits_for_cycle(candidates + committees, cycle)

    print(f"\n  Candidates: {len(candidates)}  |  Committees: {len(committees)}")
    print("  (Committee search filtered to cycle; stale committees deprioritized.)")

    exact = _exact_match(query, all_hits, cycle)
    if exact:
        print(f"\n  Exact match: {exact.name} ({exact.kind})")
        selected = exact
    else:
        print("\n  No single exact name match — pick from matches (autocomplete):")
        _print_hits(all_hits)
        if not all_hits:
            return
        selected = _prompt_choice(all_hits)
        if selected is RETYPE:
            return
        if selected is None or not isinstance(selected, SearchHit):
            return

    if selected.kind == "candidate":
        show_candidate_profile(client, selected, cycle, cycle_source)
    else:
        show_committee_profile(client, selected, cycle, cycle_source)

    # Optional: dump JSON snapshot for debugging
    save = input("\nSave raw profile JSON to file? [y/N]: ").strip().lower()
    if save == "y":
        path = f"fec_profile_{selected.entity_id}.json"
        payload: Dict[str, Any] = {"hit": selected.__dict__}
        if selected.kind == "candidate":
            detail = client.candidate_detail(selected.entity_id)
            c0 = (detail.get("results") or [{}])[0]
            office = _office_api_name(c0.get("office"))
            state = (c0.get("state") or "").upper()
            district = _district_api_value(c0.get("district"))
            race: Dict[str, Any] = {}
            if office and state:
                race = {
                    "summary": client.elections_summary(cycle, office, state, district),
                    "candidates": client.elections_in_district(
                        cycle, office, state, district
                    ),
                }
            payload.update(
                {
                    "detail": detail,
                    "cycle": cycle,
                    "cycle_source": cycle_source,
                    "totals": client.candidate_totals(selected.entity_id, cycle),
                    "race": race,
                    "outside_spending": {
                        "ie_totals": client.ie_totals_by_candidate(
                            selected.entity_id, cycle
                        ),
                        "electioneering_totals": client.electioneering_totals_by_candidate(
                            selected.entity_id, cycle
                        ),
                        "communication_costs_totals": client.communication_costs_totals_by_candidate(
                            selected.entity_id, cycle
                        ),
                        "ie_by_committee": client.ie_spenders_by_candidate(
                            selected.entity_id, cycle
                        ),
                        "electioneering_by_committee": client.electioneering_by_candidate(
                            selected.entity_id, cycle
                        ),
                        "communication_costs_by_committee": client.communication_costs_by_candidate(
                            selected.entity_id, cycle
                        ),
                        "schedule_f": client.party_coordinated_expenditures(
                            selected.entity_id, cycle
                        ),
                        "recent_schedule_e": client.recent_independent_expenditures(
                            selected.entity_id, cycle
                        ),
                        "recent_schedule_e_efile": client.recent_independent_expenditures_efile(
                            selected.entity_id
                        ),
                    },
                    "committees": client.candidate_committees(
                        selected.entity_id, cycle
                    ),
                    "filings": client.candidate_filings(selected.entity_id, cycle),
                }
            )
        else:
            detail = client.committee_detail(selected.entity_id)
            payload.update(
                {
                    "detail": detail,
                    "cycle": cycle,
                    "cycle_source": cycle_source,
                    "totals": client.committee_totals(selected.entity_id, cycle),
                    "filings": client.committee_filings(selected.entity_id, cycle),
                }
            )
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        print(f"  Wrote {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="openFEC politician search simulator")
    parser.add_argument(
        "--cycle",
        type=int,
        help="FEC two-year cycle (even year ending the period, e.g. 2026). "
        "Default: derived from today's date.",
    )
    args = parser.parse_args()

    cycle, cycle_source = resolve_target_cycle(args.cycle)

    print("openFEC politician search simulator")
    print("=" * 72)
    print(f"Target cycle: {cycle} (from {cycle_source})")
    print(f"  Resolved from date: fec_cycle_for_date() → {fec_cycle_for_date(date.today())}")
    print("Type a candidate or committee name (e.g. smith, warren, biden).")
    print("Partial names list matches; pick a number or retype a fuller name.")
    print("Override cycle: FEC_CYCLE=2024 or --cycle 2024")
    print("Empty line or 'q' to exit.\n")

    client = OpenFECClient(_api_key())

    while True:
        try:
            query = input("Search name: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break
        if not query or query.lower() in ("q", "quit", "exit"):
            print("Bye.")
            break
        try:
            run_search_flow(client, query, cycle, cycle_source)
        except HTTPError as exc:
            print(f"\nAPI error: {exc}")
        except (TimeoutError, URLError) as exc:
            if _is_timeout(exc):
                print(
                    f"\nAPI timed out (>{REQUEST_TIMEOUT}s). "
                    "Re-run or set a longer REQUEST_TIMEOUT at top of script."
                )
            else:
                print(f"\nAPI error: {exc}")
        except Exception as exc:
            print(f"\nError: {exc}")
        print("\n" + "-" * 72)


if __name__ == "__main__":
    main()
