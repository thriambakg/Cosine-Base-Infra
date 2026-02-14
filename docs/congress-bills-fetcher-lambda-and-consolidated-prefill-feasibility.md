# Feasibility: Fetcher as Lambda + Consolidated Step Function with Roll Call Delta

## 1. Switching Fetcher from Glue to Lambda

### Current (Glue)
- **Runtime**: ~6 min for entire Congress (one congress × 8 bill types = 8 ZIPs).
- **Flow**: For each (congress, bill_type), download ZIP (or use existing S3 key), save to S3 if newly downloaded, process in batches of 100 XMLs (parse with 20 workers), `put_item` per bill; date filter applied in memory (latest_action_date / introduced_date in range).
- **Resources**: Glue 4.0, 2× G.1X workers, 2-day timeout; reads ZIP from S3 or govinfo, parses in memory, writes DynamoDB + SQS (bill text).

### Lambda constraints
- **Timeout**: 15 min max per invocation.
- **Memory**: 128 MB–10 GB.
- **Ephemeral storage**: 512 MB–10 GB (`/tmp`).
- **Payload**: 6 MB sync invoke (Step Functions); response 256 KB (so state passed via S3 if large).

### Feasibility: **Yes, with one of two patterns**

**Option A – Lambda per ZIP (recommended)**  
- Step Function loops over (congress × bill_type). Each state invokes a Lambda with `{ "congress": 119, "bill_type": "hr", "zip_s3_key": "optional" }`.
- Lambda: If `zip_s3_key` in state, get ZIP from S3; else download from govinfo, then put ZIP to S3 and set `zip_s3_key` in output. Stream or chunk-read the ZIP (avoid loading entire ZIP into memory if > ~200 MB). Parse XML in batches (e.g. 100), `put_item` each batch. Return `{ "zip_s3_key": "..." }` (no manifest; roll call step will read the ZIP from S3).
- Keeps each invocation under 15 min and within memory; 8 ZIPs ⇒ 8 Lambda invocations; total wall time still on the order of a few minutes if run in parallel (or ~6 min sequential).

**Option B – Single Lambda orchestration**  
- One Lambda run: loop over congress × bill_type inside the same invocation. Must process and release each ZIP before loading the next to avoid OOM. Return the list of `zip_s3_key`s. Risk: one large ZIP or slow run could approach 15 min; need to cap work (e.g. fewer bill types per run) or accept occasional timeout.

**Recommendation**: Option A (Lambda per ZIP) is more robust. Fetcher only needs to output the S3 key(s) of the ZIP(s); the roll call Glue job reads those ZIPs from S3 and derives which bills to process from the ZIP contents.

---

## 2. Consolidating Prefill and Fetcher into a Single Step Function

### Current
- **Fetcher state machine**: Starts one Glue job (full fetcher), passes `start_date`, `end_date`, `source`; no output of which bills were updated.
- **Prefill state machine**: Starts one Glue job (roll call maintenance), which **scans the entire DynamoDB table** (all bills, exclude `SEARCH#`) and for each bill calls the Congress API (actions → recordedVotes → house-vote members). Runs on a schedule (e.g. 2 PM UTC) independent of the fetcher.

### Proposed – Single state machine
1. **Step 1 – Fetcher (Lambda or Glue)**  
   - Input: `start_date`, `end_date`, optional `congress`, etc.  
   - Does: Download/read ZIPs, parse, filter by date, `put_item`, send SQS for bill text; save each ZIP to S3 (fetcher already does this).  
   - Output (state): **S3 key(s) of the ZIP file(s)** used in this run only:  
     - `zip_s3_keys`: `["downloads/.../BILLSTATUS-119-hr.zip", "downloads/.../BILLSTATUS-119-s.zip", ...]`  
   - No manifest file or bill_id list is exported; the next step uses only the ZIP keys.

2. **Step 2 – Roll call (Glue only)**  
   - Input: `zip_s3_keys` (passed from Step 1).  
   - Glue downloads each ZIP from S3 (using the given keys), opens it, and derives the list of bill IDs from the ZIP contents (e.g. XML filenames `BILLSTATUS-119hr123.xml` → `119-HR-123`, or a lightweight parse of each XML to get `bill_id`). “Bills that are part of this run” = bills that appear in those ZIPs.  
   - Glue then processes **only those bills** for roll call: for each bill_id, get item from DynamoDB, compare `recorded_votes_json` (XML) vs `roll_call_votes` (table), call API only when there is a new roll call, merge and update (see §3).

**State transfer**  
- Fetcher (Lambda or Glue) passes **only** `zip_s3_keys` to the roll call step. The roll call Glue script reads those S3 objects (the ZIPs) and takes it from there—no separate manifest of bill_ids.

---

## 3. Roll Call Delta: Only Fetch When There Is a New Roll Call

### Current roll call behavior
- **Backfill Glue** (roll call maintenance): Scans **all** bill items (no `SEARCH#`), and for **every** bill calls the Congress API: `GET .../bill/.../actions` → extract recordedVotes → for each House vote `GET .../house-vote/.../members`. It then **overwrites** `roll_call_number` and `roll_call_votes` with the full payload (it does not merge with existing data).

### Data available
- **Bulk XML** (and therefore the table after the fetcher run): For each bill, `<recordedVotes>` / `<recordedVote>` give metadata: `chamber`, `rollNumber`, `sessionNumber`, `url`, etc. The fetcher already writes this into **`recorded_votes_json`** on the table.
- **Table**: `roll_call_votes` = JSON list of `{ "roll", "session", "members" }` (member-level vote data from the API). `roll_call_number` = first House roll number.

### Proposed delta logic (in Glue, for each bill in the run)
1. **Input**: `bill_id` (and `search_index_sk`), item from DynamoDB.
2. **From table**:  
   - `recorded_votes_json` → parse to list of votes; filter **House** only; build set `xml_rolls = { (sessionNumber, rollNumber), ... }`.  
   - `roll_call_votes` → parse to list of `{ roll, session, members }`; build set `existing_rolls = { (session, roll), ... }`.
3. **Compare**:  
   - If `xml_rolls == existing_rolls` → **skip** (no API call). E.g. bill already has roll 56, XML still only has 56; latest action might be “referred to committee” but recorded votes unchanged.  
   - If `xml_rolls != existing_rolls`:  
     - New rolls: `to_fetch = xml_rolls - existing_rolls`.  
     - For each `(session, roll)` in `to_fetch`, call API `house-vote/{congress}/{session}/{roll}/members` and get `members`.  
     - **Merge**: `new_roll_call_votes = existing_roll_call_votes + [ { "roll": r, "session": s, "members": ... } for (s,r) in to_fetch ]`.  
     - Update table: `SET roll_call_votes = :v, roll_call_number = :n` (e.g. first roll in merged list), `has_roll_call = 1`. If `xml_rolls` is empty, set `has_roll_call = 0` and REMOVE roll_call_number, roll_call_votes.
4. **No overwrite of existing member data**: We only **add** new (session, roll) entries; existing (session, roll) stay as-is (no re-fetch).

### Implementation notes
- `recorded_votes_json` uses keys like `rollNumber`, `sessionNumber` (camelCase); `roll_call_votes` uses `roll`, `session`. Normalize when building sets (e.g. both to `(session, roll)`).
- Today the backfill **replaces** `roll_call_votes` entirely; changing to the above makes it **merge** and only call the API for new rolls.

---

## 4. Summary

| Item | Feasible? | Notes |
|------|-----------|--------|
| Fetcher as Lambda | **Yes** | Prefer one Lambda per (congress, bill_type) ZIP; output `zip_s3_key`(s) for state. |
| Single Step Function | **Yes** | Step 1 = Fetcher (Lambda or Glue), Step 2 = Roll call Glue; state = `zip_s3_keys` only. |
| Roll call only for “this run’s bills” | **Yes** | Glue receives `zip_s3_keys`, reads each ZIP from S3, and derives bill_ids from the ZIP contents. |
| Roll call delta (skip if no new roll) | **Yes** | Compare `recorded_votes_json` (XML) vs `roll_call_votes` (table); API only for new (session, roll); merge into `roll_call_votes` instead of overwriting. |

### Suggested implementation order
1. **Roll call delta in Glue** – Add compare/merge logic in the existing backfill script; keep current “scan all bills” first, then switch to “scan only bills from ZIP(s)” once the pipeline passes `zip_s3_keys`.
2. **Fetcher outputs zip_s3_keys** – Whether Glue or Lambda, at end of run the fetcher passes the S3 key(s) of the ZIP file(s) it used (Glue job output / Step Function state or Lambda return). No manifest file; roll call step uses only these keys.
3. **Single state machine** – Step 1: Fetcher (current Glue or new Lambda), Step 2: Glue roll call with argument `--ZIP_S3_KEYS` (JSON array of keys). Glue downloads each ZIP from S3 and derives which bills to process from the ZIP contents.
4. **Optional: Fetcher as Lambda** – Replace Step 1 Glue with Lambda(s) per ZIP; each returns its `zip_s3_key`; state aggregates into `zip_s3_keys` for the roll call step.
