"""
AWS Glue Job: openFEC campaign finance indexing

Discovers candidates and committees for a FEC cycle, builds PROFILE items in
DynamoDB (summary data), and stores full Schedule A/B/E line items in S3 (gzip JSON).

Job modes (--MODE):
  bootstrap — index all candidates + committees active in --FEC_CYCLE
  nightly   — re-index entities with filings since --MIN_RECEIPT_DATE (default: yesterday UTC)
  single    — index one entity (--ENTITY_TYPE, --ENTITY_ID)

Secrets (--FEC_SECRET_NAME): JSON object with api_keys[] (array of API key strings).
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext

# ---------------------------------------------------------------------------
# Glue bootstrap
# ---------------------------------------------------------------------------

_REQUIRED_ARGS = [
    "JOB_NAME",
    "FEC_API_BASE_URL",
    "FEC_SECRET_NAME",
    "FEC_PROFILES_TABLE_NAME",
    "S3_BUCKET_NAME",
    "REQUEST_TIMEOUT",
    "RATE_LIMIT_DELAY",
]

args = getResolvedOptions(sys.argv, _REQUIRED_ARGS)


def _parse_optional_glue_arg(name: str) -> Optional[str]:
    """Read optional --ARG from sys.argv (Step Functions / manual overrides)."""
    flag = f"--{name}"
    for i, arg in enumerate(sys.argv):
        if arg == flag and i + 1 < len(sys.argv):
            raw = sys.argv[i + 1].strip()
            if not raw or raw.lower() in ("null", "none", ""):
                return None
            if raw.startswith("$."):
                return None
            return raw
    try:
        resolved = getResolvedOptions(sys.argv, [name])
        val = resolved.get(name)
        if val is None or str(val).strip() == "":
            return None
        return str(val).strip()
    except Exception:
        return None


def _optional_int(name: str) -> Optional[int]:
    raw = _parse_optional_glue_arg(name)
    if raw is None:
        return None
    return int(raw)


# Step Functions input: mode, cycle, min_receipt_date, testing_limit, entity_type, entity_id, source
args["SOURCE"] = _parse_optional_glue_arg("SOURCE") or "glue"
args["MODE"] = (_parse_optional_glue_arg("MODE") or "nightly").strip().lower()
args["FEC_CYCLE"] = _optional_int("FEC_CYCLE")
args["MIN_RECEIPT_DATE"] = _parse_optional_glue_arg("MIN_RECEIPT_DATE")
args["ENTITY_TYPE"] = _parse_optional_glue_arg("ENTITY_TYPE")
args["ENTITY_ID"] = _parse_optional_glue_arg("ENTITY_ID")
args["TESTING_LIMIT"] = _optional_int("TESTING_LIMIT")
_cycle_per_page = _parse_optional_glue_arg("CYCLE_PER_PAGE")
if _cycle_per_page:
    args["CYCLE_PER_PAGE"] = _cycle_per_page


def _optional_float(name: str, default: float) -> float:
    raw = _parse_optional_glue_arg(name)
    return float(raw) if raw is not None else default


# openFEC documents 1,000 requests/hour per API key; default 900 for headroom
MAX_CALLS_PER_HOUR_PER_KEY = int(_optional_float("MAX_CALLS_PER_HOUR_PER_KEY", 900))
WORKERS_PER_KEY = int(_optional_float("WORKERS_PER_KEY", 2))
MAX_PARALLEL_WORKERS = int(_optional_float("MAX_PARALLEL_WORKERS", 24))
AVG_API_CALLS_PER_ENTITY = int(_optional_float("AVG_API_CALLS_PER_ENTITY", 22))

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = logging.getLogger()
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)
logger.setLevel(logging.INFO)

API_BASE = args["FEC_API_BASE_URL"].rstrip("/")
FEC_SECRET_NAME = args["FEC_SECRET_NAME"]
TABLE_NAME = args["FEC_PROFILES_TABLE_NAME"]
S3_BUCKET = args["S3_BUCKET_NAME"]
REQUEST_TIMEOUT = int(args["REQUEST_TIMEOUT"])
SCHEDULE_REQUEST_TIMEOUT = int(
    _optional_float("SCHEDULE_REQUEST_TIMEOUT", max(REQUEST_TIMEOUT, 180))
)
RATE_LIMIT_DELAY = float(args["RATE_LIMIT_DELAY"])
CYCLE_PER_PAGE = int(args.get("CYCLE_PER_PAGE") or "100")
TESTING_LIMIT = args.get("TESTING_LIMIT")
MODE = args["MODE"]
SOURCE = args["SOURCE"]
ENTITY_TYPE = (args.get("ENTITY_TYPE") or "").strip().lower()
ENTITY_ID = (args.get("ENTITY_ID") or "").strip()

REQUEST_RETRIES = int(_optional_float("REQUEST_RETRIES", 4))
TIMEOUT_BACKOFF_BASE = float(_optional_float("TIMEOUT_BACKOFF_BASE", 5.0))
TIMEOUT_BACKOFF_MAX = float(_optional_float("TIMEOUT_BACKOFF_MAX", 90.0))
HTTP_BACKOFF_BASE = float(_optional_float("HTTP_BACKOFF_BASE", 5.0))
SLOW_FEC_PATHS = (
    "/schedules/schedule_a/",
    "/schedules/schedule_b/",
    "/schedules/schedule_e/",
)
PROFILE_SIZE_WARN_BYTES = 350_000
TOP_PAC_FUNDRAISER_LIMIT = 2
PAC_DONOR_SAMPLE = 5
RACE_RIVAL_IE_LIMIT = 2
RECENT_IE_SAMPLE = 25
SCHEMA_VERSION = 1

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")
_dynamo_write_lock = threading.Lock()
_stats_lock = threading.Lock()
_stats: Dict[str, int] = {"success": 0, "failed": 0, "api_calls": 0}


def log(msg: str) -> None:
    logger.info(msg)
    print(msg, flush=True)


def fec_cycle_for_date(on_date: Optional[date] = None) -> int:
    d = on_date or date.today()
    y = d.year
    return y if y % 2 == 0 else y + 1


def resolve_cycle() -> int:
    if args.get("FEC_CYCLE") is not None:
        return int(args["FEC_CYCLE"])
    return fec_cycle_for_date()


def _to_decimal(obj: Any) -> Any:
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_decimal(v) for v in obj]
    return obj


# ---------------------------------------------------------------------------
# API keys, per-key rate limits, parallel HTTP clients
# ---------------------------------------------------------------------------


class PerKeyRateLimiter:
    """Sliding window per API key (openFEC: 1,000 calls/hour/key)."""

    def __init__(self, max_per_hour: int) -> None:
        self.max_per_hour = max_per_hour
        self._timestamps: deque = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.time()
            while self._timestamps and self._timestamps[0] < now - 3600:
                self._timestamps.popleft()
            while len(self._timestamps) >= self.max_per_hour:
                wait = self._timestamps[0] + 3600 - now
                if wait > 0:
                    time.sleep(wait)
                now = time.time()
                while self._timestamps and self._timestamps[0] < now - 3600:
                    self._timestamps.popleft()
            self._timestamps.append(time.time())


class KeySlot:
    def __init__(self, api_key: str, index: int, max_per_hour: int) -> None:
        self.api_key = api_key
        self.index = index
        self.limiter = PerKeyRateLimiter(max_per_hour)


class FecClientPool:
    """Dedicated key + rate bucket per slot; worker N uses slot N % num_keys."""

    def __init__(self, keys: List[str], max_per_hour_per_key: int) -> None:
        if not keys:
            raise ValueError("No FEC API keys")
        self.slots = [
            KeySlot(k, i, max_per_hour_per_key) for i, k in enumerate(keys)
        ]

    @property
    def num_keys(self) -> int:
        return len(self.slots)

    def client_for_worker(self, worker_id: int, context: str) -> "FecClient":
        slot = self.slots[worker_id % len(self.slots)]
        prefix = f"[key {slot.index + 1}/{len(self.slots)} {context}]"
        return FecClient(slot, log_prefix=prefix)

    def discovery_client(self) -> "FecClient":
        return self.client_for_worker(0, "discovery")


def resolve_parallel_workers(num_keys: int, total_entities: int) -> int:
    if total_entities <= 0:
        return 1
    return max(
        1,
        min(num_keys * WORKERS_PER_KEY, MAX_PARALLEL_WORKERS, total_entities),
    )


def log_capacity_plan(num_keys: int, total_entities: int, workers: int) -> None:
    hourly_budget = num_keys * MAX_CALLS_PER_HOUR_PER_KEY
    est_calls = total_entities * AVG_API_CALLS_PER_ENTITY
    est_hours = est_calls / hourly_budget if hourly_budget else 0
    log(
        f"Parallelism: {workers} threads from {num_keys} key(s) "
        f"(×{WORKERS_PER_KEY} workers/key, cap {MAX_PARALLEL_WORKERS})"
    )
    log(
        f"Rate budget: {hourly_budget:,} API calls/hour "
        f"({MAX_CALLS_PER_HOUR_PER_KEY}/key; openFEC hard limit 1,000/key)"
    )
    if total_entities:
        log(
            f"Rough ETA: ~{est_hours:.1f}h for {total_entities} entities "
            f"(~{AVG_API_CALLS_PER_ENTITY} calls each, ~{est_calls:,} total)"
        )
    log(
        "Keys: 10–15 is usually enough for bootstrap; add keys if logs show 429s or "
        "long limiter waits. Threads cap ~24 on G.1X driver."
    )


def load_api_keys(secret_name: str) -> List[str]:
    """Secret JSON: { \"api_keys\": [\"key1\", \"key2\"] } (Terraform stores api_keys as JSON string)."""
    resp = secrets_client.get_secret_value(SecretId=secret_name)
    payload = json.loads(resp["SecretString"])
    raw_keys: Any = payload.get("api_keys") if isinstance(payload, dict) else payload
    if isinstance(raw_keys, str) and raw_keys.strip():
        try:
            raw_keys = json.loads(raw_keys)
        except json.JSONDecodeError:
            raw_keys = [raw_keys]
    if not isinstance(raw_keys, list):
        raise ValueError(
            "FEC secret must contain api_keys as a JSON array "
            '(e.g. {"api_keys": ["key1", "key2"]})'
        )
    keys = [str(k).strip() for k in raw_keys if str(k).strip()]
    if not keys:
        raise ValueError("FEC secret api_keys array is empty")
    log(f"Loaded {len(keys)} FEC API key(s) from Secrets Manager")
    return keys


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, URLError):
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            return True
        return "timed out" in str(exc).lower()
    return False


def _request_timeout_for(path: str, override: Optional[int] = None) -> int:
    if override is not None:
        return override
    if any(path.startswith(prefix) for prefix in SLOW_FEC_PATHS):
        return SCHEDULE_REQUEST_TIMEOUT
    return REQUEST_TIMEOUT


def _timeout_backoff_seconds(attempt: int) -> float:
    return min(TIMEOUT_BACKOFF_MAX, TIMEOUT_BACKOFF_BASE * (2**attempt))


def _http_backoff_seconds(attempt: int) -> float:
    return max(HTTP_BACKOFF_BASE, HTTP_BACKOFF_BASE * (2**attempt))


class FecClient:
    def __init__(self, slot: KeySlot, log_prefix: str = "") -> None:
        self.slot = slot
        self.log_prefix = log_prefix

    def _record_call(self) -> None:
        with _stats_lock:
            _stats["api_calls"] += 1

    def get(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        allow_404: bool = False,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        self.slot.limiter.acquire()
        if RATE_LIMIT_DELAY > 0:
            time.sleep(RATE_LIMIT_DELAY)
        query = dict(params or {})
        query["api_key"] = self.slot.api_key
        url = f"{API_BASE}{path}?{urlencode(query, doseq=True)}"
        req = Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "Cosine-openFEC-glue/1.0",
            },
        )
        wait = _request_timeout_for(path, timeout)
        last_err: Optional[BaseException] = None
        for attempt in range(REQUEST_RETRIES + 1):
            try:
                with urlopen(req, timeout=wait) as resp:
                    self._record_call()
                    return json.loads(resp.read().decode("utf-8"))
            except HTTPError as exc:
                if allow_404 and exc.code == 404:
                    self._record_call()
                    return {"results": [], "pagination": {"count": 0}}
                if exc.code in (429, 500, 502, 503, 504) and attempt < REQUEST_RETRIES:
                    delay = _http_backoff_seconds(attempt)
                    log(
                        f"{self.log_prefix} HTTP {exc.code} {path} — "
                        f"retry in {delay:.0f}s (attempt {attempt + 1}/{REQUEST_RETRIES})"
                    )
                    time.sleep(delay)
                    self.slot.limiter.acquire()
                    last_err = exc
                    continue
                body = exc.read().decode("utf-8", errors="replace")[:300]
                raise RuntimeError(f"HTTP {exc.code} {path}: {body}") from exc
            except (TimeoutError, URLError) as exc:
                if not _is_timeout(exc):
                    raise
                last_err = exc
                if attempt < REQUEST_RETRIES:
                    delay = _timeout_backoff_seconds(attempt)
                    log(
                        f"{self.log_prefix} Timeout {path} ({wait}s) — "
                        f"retry in {delay:.0f}s (attempt {attempt + 1}/{REQUEST_RETRIES})"
                    )
                    time.sleep(delay)
                    continue
                raise
        if last_err:
            raise last_err
        raise RuntimeError(f"Failed to fetch {path}")

    def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        allow_404: bool = False,
        label: str = "",
        timeout: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch pages; set max_pages=1 for samples (test script uses a single GET)."""
        base = dict(params or {})
        base.setdefault("per_page", CYCLE_PER_PAGE)
        page = 1
        all_rows: List[Dict[str, Any]] = []
        while True:
            base["page"] = page
            data = self.get(path, base, allow_404=allow_404, timeout=timeout)
            rows = data.get("results") or []
            pagination = data.get("pagination") or {}
            pages = int(pagination.get("pages") or 1) or 1
            tag = label or path
            log(f"{self.log_prefix} FEC {tag} page {page}/{pages} — ok ({len(rows)} rows)")
            all_rows.extend(rows)
            if max_pages is not None and page >= max_pages:
                break
            if page >= pages:
                break
            page += 1
        return all_rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _office_api(office_code: Optional[str]) -> Optional[str]:
    return {"H": "house", "S": "senate", "P": "president"}.get(
        str(office_code or "").upper()
    )


def _district_value(district: Any) -> str:
    if district is None or district == "":
        return "00"
    try:
        return f"{int(district):02d}"
    except (TypeError, ValueError):
        text = str(district).strip()
        return text if len(text) == 2 else text.zfill(2)


def _pick_totals_row(rows: List[Dict[str, Any]], cycle: int) -> Dict[str, Any]:
    if not rows:
        return {}
    for row in rows:
        if row.get("cycle") == cycle:
            return row
    return rows[0]


def _is_super_pac(committee: Dict[str, Any]) -> bool:
    return str(committee.get("committee_type") or "").upper() == "O"


def _schedule_s3_key(cycle: int, committee_id: str, schedule: str) -> str:
    return f"{cycle}/committee/{committee_id}/{schedule}.json.gz"


def write_schedule_s3(
    cycle: int,
    committee_id: str,
    schedule: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    key = _schedule_s3_key(cycle, committee_id, schedule)
    raw = json.dumps(rows, default=str).encode("utf-8")
    body = gzip.compress(raw)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/gzip",
        ContentEncoding="gzip",
    )
    log(f"S3 wrote s3://{S3_BUCKET}/{key} ({len(rows)} rows, {len(body)} bytes gzip)")
    return {
        "s3_bucket": S3_BUCKET,
        "s3_key": key,
        "row_count": len(rows),
        "byte_size": len(body),
        "content_sha256": hashlib.sha256(body).hexdigest(),
        "indexed_at": datetime.now(timezone.utc).isoformat(),
    }


def put_profile(entity_type: str, entity_id: str, cycle: int, profile: Dict[str, Any]) -> None:
    pk = f"{'CANDIDATE' if entity_type == 'candidate' else 'COMMITTEE'}#{entity_id}"
    sk = f"PROFILE#{cycle}"
    item = {"PK": pk, "SK": sk, **profile}
    raw_size = len(json.dumps(item, default=str))
    if raw_size > PROFILE_SIZE_WARN_BYTES:
        log(f"WARNING {pk} {sk} size {raw_size} exceeds {PROFILE_SIZE_WARN_BYTES} bytes")
    with _dynamo_write_lock:
        table.put_item(Item=_to_decimal(item))
    log(f"DynamoDB put {pk} {sk} ({raw_size} bytes)")


def _max_date(values: List[Optional[str]]) -> Optional[str]:
    clean = [v[:10] for v in values if v]
    return max(clean) if clean else None


# ---------------------------------------------------------------------------
# Profile builders
# ---------------------------------------------------------------------------


def build_committee_profile(
    client: FecClient, committee_id: str, cycle: int
) -> Dict[str, Any]:
    detail_resp = client.get(f"/committee/{committee_id}/")
    detail = (detail_resp.get("results") or [{}])[0]
    totals = _pick_totals_row(
        client.paginate(
            f"/committee/{committee_id}/totals/",
            {"cycle": cycle, "sort": "-cycle"},
            allow_404=True,
            label=f"committee/{committee_id}/totals",
        ),
        cycle,
    )
    filings = client.paginate(
        f"/committee/{committee_id}/filings/",
        {"cycle": cycle, "sort": "-receipt_date"},
        label=f"committee/{committee_id}/filings",
    )
    reports = client.paginate(
        f"/committee/{committee_id}/reports/",
        {"cycle": cycle, "is_amended": "false", "sort": "-coverage_end_date"},
        allow_404=True,
        label=f"committee/{committee_id}/reports",
    )
    linked = client.paginate(
        f"/committee/{committee_id}/candidates/",
        {"cycle": cycle},
        label=f"committee/{committee_id}/candidates",
    )
    by_size = client.paginate(
        "/schedules/schedule_a/by_size/",
        {"committee_id": committee_id, "two_year_transaction_period": cycle},
        label=f"schedule_a/by_size {committee_id}",
    )
    by_recipient = client.paginate(
        "/schedules/schedule_b/by_recipient/",
        {"committee_id": committee_id, "two_year_transaction_period": cycle},
        label=f"schedule_b/by_recipient {committee_id}",
    )
    by_purpose = client.paginate(
        "/schedules/schedule_b/by_purpose/",
        {"committee_id": committee_id, "two_year_transaction_period": cycle},
        label=f"schedule_b/by_purpose {committee_id}",
    )

    sched_a_rows = client.paginate(
        "/schedules/schedule_a/",
        {
            "committee_id": committee_id,
            "two_year_transaction_period": cycle,
            "sort": "-contribution_receipt_date",
        },
        label=f"schedule_a {committee_id}",
    )
    sched_b_rows = client.paginate(
        "/schedules/schedule_b/",
        {
            "committee_id": committee_id,
            "two_year_transaction_period": cycle,
            "sort": "-disbursement_date",
        },
        label=f"schedule_b {committee_id}",
    )

    schedules: Dict[str, Any] = {
        "schedule_a": write_schedule_s3(cycle, committee_id, "schedule_a", sched_a_rows),
        "schedule_b": write_schedule_s3(cycle, committee_id, "schedule_b", sched_b_rows),
        "schedule_e": None,
    }

    if _is_super_pac(detail):
        sched_e = client.paginate(
            "/schedules/schedule_e/",
            {"committee_id": committee_id, "sort": "-expenditure_date"},
            label=f"schedule_e {committee_id}",
        )
        schedules["schedule_e"] = write_schedule_s3(
            cycle, committee_id, "schedule_e", sched_e
        )

    filing_dates = [
        (f.get("receipt_date") or f.get("coverage_end_date") or "") for f in filings
    ]
    now = datetime.now(timezone.utc).isoformat()

    return {
        "item_type": "COMMITTEE_PROFILE",
        "entity_id": committee_id,
        "cycle": cycle,
        "schema_version": SCHEMA_VERSION,
        "index_meta": {
            "last_indexed_at": now,
            "index_mode": MODE,
            "max_filing_receipt_date": _max_date(filing_dates),
            "max_report_coverage_end": _max_date(
                [r.get("coverage_end_date") for r in reports]
            ),
            "linked_candidate_ids": [
                c.get("candidate_id") for c in linked if c.get("candidate_id")
            ],
        },
        "detail": detail,
        "totals": totals,
        "filings": {
            "latest_receipt_date": filing_dates[0] if filing_dates else None,
            "count": len(filings),
            "items": filings[:50],
        },
        "linked_candidates": linked,
        "reports": reports,
        "donor_aggregates": {"by_size": by_size},
        "disbursement_aggregates": {
            "by_recipient": by_recipient,
            "by_purpose": by_purpose,
        },
        "schedules": schedules,
    }


def build_candidate_profile(
    client: FecClient, candidate_id: str, cycle: int
) -> Dict[str, Any]:
    detail_resp = client.get(f"/candidate/{candidate_id}/")
    detail = (detail_resp.get("results") or [{}])[0]
    authorized = _pick_totals_row(
        client.paginate(
            f"/candidate/{candidate_id}/totals/",
            {"cycle": cycle, "sort": "-cycle"},
            allow_404=True,
            label=f"candidate/{candidate_id}/totals",
        ),
        cycle,
    )
    committees = client.paginate(
        f"/candidate/{candidate_id}/committees/",
        {"cycle": cycle},
        label=f"candidate/{candidate_id}/committees",
    )
    filings = client.paginate(
        f"/candidate/{candidate_id}/filings/",
        {"cycle": cycle, "sort": "-receipt_date"},
        label=f"candidate/{candidate_id}/filings",
    )

    principal = next(
        (c for c in committees if c.get("designation") == "P"),
        committees[0] if committees else None,
    )
    principal_id = principal.get("committee_id") if principal else None

    by_cand_size = client.paginate(
        "/schedules/schedule_a/by_size/by_candidate/",
        {"candidate_id": candidate_id, "cycle": cycle},
        label=f"schedule_a/by_size/by_candidate {candidate_id}",
    )
    by_cand_state = client.paginate(
        "/schedules/schedule_a/by_state/by_candidate/totals/",
        {"candidate_id": candidate_id, "cycle": cycle},
        label=f"schedule_a/by_state {candidate_id}",
    )

    principal_by_size: List[Dict[str, Any]] = []
    if principal_id:
        principal_by_size = client.paginate(
            "/schedules/schedule_a/by_size/",
            {
                "committee_id": principal_id,
                "two_year_transaction_period": cycle,
            },
            label=f"schedule_a/by_size {principal_id}",
        )

    office = _office_api(detail.get("office"))
    state = (detail.get("state") or "").upper()
    district = _district_value(detail.get("district"))

    race: Dict[str, Any] = {}
    if office and state:
        summary_rows = client.get(
            "/elections/summary/",
            {
                "cycle": cycle,
                "office": office,
                "state": state,
                "district": district,
            },
            allow_404=True,
        ).get("results") or []
        racers = client.paginate(
            "/elections/",
            {"cycle": cycle, "office": office, "state": state, "district": district},
            label=f"elections {state}-{district}",
        )
        rivals = sorted(
            [r for r in racers if r.get("candidate_id") != candidate_id],
            key=lambda r: float(r.get("total_receipts") or 0),
            reverse=True,
        )[:RACE_RIVAL_IE_LIMIT]
        for row in rivals:
            cid = row.get("candidate_id")
            if not cid:
                continue
            try:
                ie = client.get(
                    "/schedules/schedule_e/totals/by_candidate/",
                    {
                        "candidate_id": cid,
                        "cycle": cycle,
                        "election_full": "true",
                    },
                    allow_404=True,
                ).get("results") or []
                row["ie_totals"] = ie
            except Exception as exc:
                log(f"IE totals for rival {cid} skipped: {exc}")

        for row in racers:
            if row.get("candidate_id") == candidate_id:
                row["is_subject"] = True

        race = {
            "office": office,
            "state": state,
            "district": district,
            "cycle": cycle,
            "summary": summary_rows[0] if summary_rows else {},
            "candidates_in_seat": racers,
        }

    ie_totals = client.get(
        "/schedules/schedule_e/totals/by_candidate/",
        {
            "candidate_id": candidate_id,
            "cycle": cycle,
            "election_full": "true",
        },
        allow_404=True,
    ).get("results") or []

    support_total = sum(
        float(r.get("total") or 0)
        for r in ie_totals
        if r.get("support_oppose_indicator") == "S"
    )
    oppose_total = sum(
        float(r.get("total") or 0)
        for r in ie_totals
        if r.get("support_oppose_indicator") == "O"
    )

    spenders = client.paginate(
        "/schedules/schedule_e/by_candidate/",
        {"candidate_id": candidate_id, "cycle": cycle},
        label=f"schedule_e/by_candidate {candidate_id}",
    )

    top_outside: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for row in sorted(spenders, key=lambda r: float(r.get("total") or 0), reverse=True):
        cid = row.get("committee_id")
        if not cid or cid in seen:
            continue
        seen.add(cid)
        pac_totals = _pick_totals_row(
            client.paginate(
                f"/committee/{cid}/totals/",
                {"cycle": cycle},
                allow_404=True,
                label=f"outside PAC totals {cid}",
            ),
            cycle,
        )
        try:
            donors = client.paginate(
                "/schedules/schedule_a/",
                {
                    "committee_id": cid,
                    "two_year_transaction_period": cycle,
                    "per_page": PAC_DONOR_SAMPLE,
                    "sort": "-contribution_receipt_date",
                },
                label=f"outside PAC donors {cid}",
                max_pages=1,
            )[:PAC_DONOR_SAMPLE]
        except (TimeoutError, URLError) as exc:
            if _is_timeout(exc):
                log(
                    f"{client.log_prefix} outside PAC donors {cid} skipped "
                    f"(timeout after {SCHEDULE_REQUEST_TIMEOUT}s)"
                )
                donors = []
            else:
                raise
        top_outside.append(
            {
                "committee_id": cid,
                "committee_name": row.get("committee_name"),
                "ie_total": row.get("total"),
                "support_oppose_indicator": row.get("support_oppose_indicator"),
                "pac_totals": pac_totals,
                "recent_contributions_sample": donors,
                "committee_profile_pk": f"COMMITTEE#{cid}",
                "committee_profile_sk": f"PROFILE#{cycle}",
            }
        )
        if len(top_outside) >= TOP_PAC_FUNDRAISER_LIMIT:
            break

    try:
        recent_e = client.paginate(
            "/schedules/schedule_e/",
            {
                "candidate_id": candidate_id,
                "two_year_transaction_period": cycle,
                "per_page": RECENT_IE_SAMPLE,
                "sort": "-expenditure_date",
            },
            label=f"schedule_e {candidate_id}",
            max_pages=1,
        )[:RECENT_IE_SAMPLE]
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            log(f"{client.log_prefix} recent schedule_e skipped (timeout)")
            recent_e = []
        else:
            raise

    try:
        recent_efile = client.paginate(
            "/schedules/schedule_e/efile/",
            {
                "candidate_id": candidate_id,
                "per_page": RECENT_IE_SAMPLE,
                "sort": "-expenditure_date",
            },
            label=f"schedule_e/efile {candidate_id}",
            max_pages=1,
        )[:RECENT_IE_SAMPLE]
    except (TimeoutError, URLError) as exc:
        if _is_timeout(exc):
            log(f"{client.log_prefix} recent schedule_e/efile skipped (timeout)")
            recent_efile = []
        else:
            raise

    now = datetime.now(timezone.utc).isoformat()
    ie_dates = [
        r.get("expenditure_date") or r.get("dissemination_date") for r in recent_e + recent_efile
    ]

    profile: Dict[str, Any] = {
        "item_type": "CANDIDATE_PROFILE",
        "entity_id": candidate_id,
        "cycle": cycle,
        "schema_version": SCHEMA_VERSION,
        "index_meta": {
            "last_indexed_at": now,
            "index_mode": MODE,
            "max_filing_receipt_date": _max_date(
                [(f.get("receipt_date") or "") for f in filings]
            ),
            "max_schedule_e_date": _max_date(ie_dates),
            "principal_committee_id": principal_id,
        },
        "detail": detail,
        "authorized_totals": authorized,
        "race": race,
        "outside_spending": {
            "support_total": support_total,
            "oppose_total": oppose_total,
            "exclude_24_48hr_in_aggregates": True,
            "by_committee": spenders,
            "top_outside_committees": top_outside,
            "recent_schedule_e": recent_e,
            "recent_schedule_e_efile": recent_efile,
        },
        "committees": committees,
        "filings": filings,
        "donor_aggregates": {
            "all_linked_committees": {
                "by_size": by_cand_size,
                "by_state": by_cand_state,
            },
            "principal_by_size": principal_by_size,
        },
        "principal_committee_id": principal_id,
    }
    if principal_id:
        profile["principal_committee_ref"] = {
            "committee_id": principal_id,
            "name": principal.get("name"),
            "profile_pk": f"COMMITTEE#{principal_id}",
            "profile_sk": f"PROFILE#{cycle}",
        }
    return profile


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def discover_bootstrap(pool: FecClientPool, cycle: int) -> Tuple[List[str], List[str]]:
    client = pool.discovery_client()
    log(f"Discovering all candidates and committees for cycle {cycle}")
    candidates = client.paginate(
        "/candidates/",
        {"cycle": cycle, "sort": "name"},
        label="/candidates/",
    )
    committees = client.paginate(
        "/committees/",
        {"cycle": cycle, "sort": "name"},
        label="/committees/",
    )
    c_ids = sorted({r.get("candidate_id") for r in candidates if r.get("candidate_id")})
    comm_ids = sorted({r.get("committee_id") for r in committees if r.get("committee_id")})
    log(f"Discovery complete: {len(c_ids)} candidates, {len(comm_ids)} committees")
    return c_ids, comm_ids


def discover_nightly(
    pool: FecClientPool, cycle: int, min_receipt_date: str
) -> Tuple[List[str], List[str]]:
    client = pool.discovery_client()
    log(f"Nightly discovery: filings since {min_receipt_date} (cycle {cycle})")
    filings = client.paginate(
        "/filings/",
        {
            "cycle": cycle,
            "min_receipt_date": min_receipt_date,
            "sort": "-receipt_date",
        },
        label="/filings/",
    )
    c_ids: set[str] = set()
    comm_ids: set[str] = set()
    for row in filings:
        cid_field = row.get("candidate_id")
        if cid_field:
            if isinstance(cid_field, list):
                c_ids.update(str(x) for x in cid_field if x)
            else:
                c_ids.add(str(cid_field))
        if row.get("committee_id"):
            comm_ids.add(str(row["committee_id"]))
        for cid in row.get("candidate_ids") or []:
            if cid:
                c_ids.add(str(cid))
    log(f"Nightly discovery: {len(c_ids)} candidates, {len(comm_ids)} committees from filings")
    return sorted(c_ids), sorted(comm_ids)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def index_entity(client: FecClient, entity_type: str, entity_id: str, cycle: int) -> None:
    log(f"{client.log_prefix} START index (cycle {cycle})")
    if entity_type == "candidate":
        profile = build_candidate_profile(client, entity_id, cycle)
    elif entity_type == "committee":
        profile = build_committee_profile(client, entity_id, cycle)
    else:
        raise ValueError(f"Unknown entity_type: {entity_type}")
    put_profile(entity_type, entity_id, cycle, profile)
    log(f"{client.log_prefix} SUCCESS")


def _index_entity_task(
    pool: FecClientPool,
    worker_id: int,
    entity_type: str,
    entity_id: str,
    cycle: int,
) -> Tuple[str, str, bool, Optional[str]]:
    client = pool.client_for_worker(worker_id, f"{entity_type}/{entity_id}")
    try:
        index_entity(client, entity_type, entity_id, cycle)
        with _stats_lock:
            _stats["success"] += 1
        return entity_type, entity_id, True, None
    except Exception as exc:
        with _stats_lock:
            _stats["failed"] += 1
        log(f"{client.log_prefix} FAILED — {exc}")
        return entity_type, entity_id, False, str(exc)


def _run_parallel_index(
    pool: FecClientPool,
    tasks: List[Tuple[str, str]],
    cycle: int,
) -> List[Tuple[str, str, bool, Optional[str]]]:
    workers = resolve_parallel_workers(pool.num_keys, len(tasks))
    log_capacity_plan(pool.num_keys, len(tasks), workers)
    results: List[Tuple[str, str, bool, Optional[str]]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _index_entity_task, pool, i % workers, etype, eid, cycle
            ): (etype, eid)
            for i, (etype, eid) in enumerate(tasks)
        }
        done = 0
        for future in as_completed(futures):
            results.append(future.result())
            done += 1
            if done % 50 == 0 or done == len(tasks):
                log(f"Progress: {done}/{len(tasks)} entities finished")
    return results


def main() -> None:
    cycle = resolve_cycle()
    min_date = args.get("MIN_RECEIPT_DATE")
    if not min_date and MODE == "nightly":
        min_date = (date.today() - timedelta(days=1)).isoformat()

    log("=" * 72)
    log(f"openFEC Glue job started — source={SOURCE} mode={MODE} cycle={cycle}")
    if min_date:
        log(f"min_receipt_date={min_date}")
    log(f"Table={TABLE_NAME} S3={S3_BUCKET}")
    log("=" * 72)

    api_keys = load_api_keys(FEC_SECRET_NAME)
    pool = FecClientPool(api_keys, MAX_CALLS_PER_HOUR_PER_KEY)

    if MODE == "single":
        if not ENTITY_TYPE or not ENTITY_ID:
            raise ValueError("MODE=single requires --ENTITY_TYPE and --ENTITY_ID")
        client = pool.client_for_worker(0, f"{ENTITY_TYPE}/{ENTITY_ID}")
        index_entity(client, ENTITY_TYPE, ENTITY_ID, cycle)
        log("Job finished (single entity)")
        job.commit()
        return

    if MODE == "bootstrap":
        candidate_ids, committee_ids = discover_bootstrap(pool, cycle)
    elif MODE == "nightly":
        candidate_ids, committee_ids = discover_nightly(pool, cycle, min_date)
    else:
        raise ValueError(f"Unknown MODE: {MODE} (use bootstrap, nightly, or single)")

    if TESTING_LIMIT:
        candidate_ids = candidate_ids[:TESTING_LIMIT]
        committee_ids = committee_ids[:TESTING_LIMIT]
        log(f"TESTING_LIMIT={TESTING_LIMIT} applied")

    tasks: List[Tuple[str, str]] = (
        [("candidate", cid) for cid in candidate_ids]
        + [("committee", cid) for cid in committee_ids]
    )
    log(
        f"Queue: {len(candidate_ids)} candidates + {len(committee_ids)} committees "
        f"({len(tasks)} total)"
    )

    results = _run_parallel_index(pool, tasks, cycle)
    failed = [(t, e, err) for t, e, ok, err in results if not ok]
    log(
        f"Job finished — success={_stats['success']} failed={_stats['failed']} "
        f"api_calls={_stats['api_calls']} cycle={cycle}"
    )
    if failed:
        log(f"Failed entities ({len(failed)}) — first 10:")
        for entity_type, entity_id, err in failed[:10]:
            log(f"  {entity_type} {entity_id}: {err}")
        raise RuntimeError(f"{len(failed)} entities failed indexing")
    job.commit()


if __name__ == "__main__":
    main()
