# Congress Bills Glue Jobs

Per-job layout:

- **fetcher/** – Bulk bill fetch from govinfo, write to DynamoDB. Uses defusedxml via `--additional-python-modules`; deps listed in `static-files/glue_deps/requirements.txt` (uploaded to S3 by Terraform). Parses Bill Status XML `<recordedVotes>` (and per-action votes in current format) and action links; sets `has_roll_call` and `recorded_votes_json` so bills with new roll calls are updated when re-downloaded via latest-action date.
- **crawler/** – Backfill bill text and roll call data (Congress.gov API). No extra Python deps; Glue runtime only.

Terraform expects scripts at:

- `s3://.../congress_bills/fetcher/glue_script.py`
- `s3://.../congress_bills/crawler/backfill_bill_text.py`
- `s3://.../glue_deps/requirements.txt` (static-files/glue_deps/requirements.txt; single source of truth for Glue deps)

**Bulk batch date vs bill `updateDate`:** The fetcher writes ZIPs to `downloads/{YYYYMMDD-YYYYMMDD}/` by the *job run* date range. Each ZIP is the full BILLSTATUS set for that Congress and bill type (e.g. all 119th House bills), not filtered by “updated on this day.” So a bill whose XML has `updateDate` 02-04 can appear in a 02/13–02/14 batch; that’s expected.

**Actions:** Bulk XML has one `<item>` per action per source (House floor, Library of Congress, etc.), so the same event can appear multiple times. The fetcher deduplicates by `(actionDate, text)` before storing so `actions_json` matches Congress.gov’s one-row-per-event display.

**Filtering by action date (no server-side option):** The [BILLSTATUS XML User Guide](https://www.govinfo.gov/bulkdata/BILLSTATUS) and govinfo bulk repository do **not** support filtering by action date or “updated since” at download time. Data is organized only by **Congress** and **bill type**; each ZIP is the full set for that Congress/type. To avoid full-table scans and unnecessary DynamoDB writes on scheduled runs, the fetcher (1) downloads the full ZIP per Congress/type, (2) parses XML in batches, (3) **filters in memory** by `latest_action_date` (or `introduced_date`) so only bills whose latest action (or intro) falls in the job’s date range are written. So DynamoDB only receives PutItem for bills in range; there is no table scan. The unavoidable cost is full ZIP download and parse per run; there is no incremental bulk API from govinfo.
