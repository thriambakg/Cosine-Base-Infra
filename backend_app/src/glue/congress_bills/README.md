# Congress Bills Glue Jobs

Per-job layout:

- **fetcher/** – Bulk bill fetch from govinfo, write to DynamoDB. Uses defusedxml via `--additional-python-modules`; deps listed in `static-files/glue_deps/requirements.txt` (uploaded to S3 by Terraform).
- **crawler/** – Backfill bill text and roll call data (Congress.gov API). No extra Python deps; Glue runtime only.

Terraform expects scripts at:

- `s3://.../congress_bills/fetcher/glue_script.py`
- `s3://.../congress_bills/crawler/backfill_bill_text.py`
- `s3://.../glue_deps.zip` (archive of static-files/glue_deps/requirements.txt; single source of truth for Glue deps)
