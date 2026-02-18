# SEARCH#VOTE and SEARCH#ROLL: Member search and resolving bill/roll clicks

This doc describes how to let users **search a congress member**, get their **bills and roll calls** they voted on, and **resolve bill IDs and roll call IDs** to the underlying data via primary key and search index lookups. All data lives in the **same DynamoDB table** (congress bills table).

---

## 1. Table key schema (same for all item types)

- **Partition key (PK):** `bill_id` (String)
- **Sort key (SK):** `search_index_sk` (String)

---

## 2. Member search → list of bills and roll calls

**Goal:** User searches for a congress member and sees lists of bills and roll calls they voted on.

- **Lookup:** One item per member.  
  **PK:** `SEARCH#VOTE#<politician_id>`  
  **SK:** `VOTE`  
  Example: `bill_id = "SEARCH#VOTE#B000740"`, `search_index_sk = "VOTE"`.

- **GetItem:**  
  `Key = { "bill_id": "SEARCH#VOTE#<politician_id>", "search_index_sk": "VOTE" }`

- **Attributes (lists):**
  - **Bills:** `bill_yea`, `bill_nea`, `bill_abstained` — each a list of **bill IDs** (e.g. `"119-HR-1286"`).
  - **Roll calls:** `roll_yea`, `roll_nea`, `roll_abstained` — each a list of **roll IDs** (e.g. `"119#2#9"` = congress#session#roll).
  - **Display:** `display_name`, `search_type`, `search_value`.

- **Oversize:** If the item has `vote_data_oversize_s3_key`, the six lists above are stored in S3 (gzip JSON). Fetch that object and parse; keys in the JSON are `bill_yea`, `bill_nea`, `bill_abstained`, `roll_yea`, `roll_nea`, `roll_abstained`.

**Politician ID:** Use `bioguide_id` from the legislators CSV (e.g. `B000740`). The backfill seeds one SEARCH#VOTE item per legislator using that id.

---

## 3. Resolving a bill ID (user clicks a bill)

**Goal:** When the user clicks a bill ID (e.g. `119-HR-1286`), load the bill’s underlying record.

- **Lookup:** Primary key on the **same table**. The main bill row uses the bill ID as both PK and SK.
- **GetItem:**  
  `Key = { "bill_id": "<bill_id>", "search_index_sk": "<bill_id>" }`  
  Example: `bill_id = "119-HR-1286"`, `search_index_sk = "119-HR-1286"`.

So **bill IDs from SEARCH#VOTE map directly to a single GetItem**; no search index is required.

---

## 4. Resolving a roll call ID (user clicks a roll call)

**Goal:** When the user clicks a roll ID (e.g. `119#2#9`), load the roll call’s underlying record.

- **Lookup:** Search index item. All roll call rows share the same partition and use the roll id as the sort key.
- **PK:** `SEARCH#ROLL`
- **SK:** `{congress}#{session}#{roll}` — this is exactly the **roll ID** stored in `roll_yea` / `roll_nea` / `roll_abstained` (e.g. `"119#2#9"`).

- **GetItem:**  
  `Key = { "bill_id": "SEARCH#ROLL", "search_index_sk": "<roll_id>" }`  
  Example: `bill_id = "SEARCH#ROLL"`, `search_index_sk = "119#2#9"`.

So **roll IDs from SEARCH#VOTE map directly to a single GetItem** on the same table; again, no extra search index is required beyond this key design.

- **Attributes:** e.g. `congress`, `session`, `roll`, `bill_id_associated`, `roll_display`, `members` or `members_oversize_s3_key` (if oversize, fetch member list from S3).

---

## 5. Summary: can we map to the underlying data?

| User action              | Id from SEARCH#VOTE   | DynamoDB lookup                                                                 |
|--------------------------|-----------------------|----------------------------------------------------------------------------------|
| Search member            | (name / bioguide)     | GetItem PK=`SEARCH#VOTE#<politician_id>`, SK=`VOTE`                             |
| Click bill ID            | e.g. `119-HR-1286`    | GetItem PK=`119-HR-1286`, SK=`119-HR-1286`                                      |
| Click roll call ID       | e.g. `119#2#9`        | GetItem PK=`SEARCH#ROLL`, SK=`119#2#9`                                          |

So **yes**: searching a member and then resolving bill IDs and roll call IDs to the underlying data is done via **primary key GetItem** on the same table in both cases; no separate search index is needed beyond the PK/SK design above.

---

## 6. Scheduled runs and SEARCH#VOTE updates

**Do subsequent scheduled runs overwrite the SEARCH#VOTE index?**

- **No.** Each run only updates SEARCH#VOTE items for politicians who voted on the bills/rolls processed in that run. Other politicians’ items are never touched.
- For each politician updated, the backfill **merges** with existing data: it BatchGets the current item (or loads from S3 when `vote_data_oversize_s3_key` is set), unions the new bill/roll IDs with existing lists, then Puts the merged item. So runs are additive and idempotent for the same bill/roll.
- **Causes of discrepancies:** (1) S3 load failure for an oversize item used to fall back to empty lists and could overwrite that politician’s data with only the current run’s votes — the backfill now re-raises on S3 failure so it does not overwrite. (2) Concurrent runs updating the same politician can cause a lost update (one run’s write overwrites the other’s). (3) Politician ID mismatch (e.g. bioguide_id vs `NAME#...` when not in CSV) can split one member’s history across two SEARCH#VOTE items.
