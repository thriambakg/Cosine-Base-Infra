# Roll call and SEARCH#VOTE flow validation

## 1. Current flow (validated)

### Per bill in the backfill

1. **GetItem** the bill → read `recorded_votes_json` (XML roll list) and existing `roll_call_votes`.
2. **Compare** XML roll set vs existing roll set. If **identical** → skip this bill (no API calls, no SEARCH#VOTE update).
3. If **different**: compute `to_fetch = xml_rolls - existing_rolls` (only rolls we don’t have).
4. **API**: For each `(session, roll)` in `to_fetch` only, call **one** House vote-members API (`_fetch_house_vote_members`). So we do **one API call per new roll**, not per bill and not per roll when we already have it.
5. **Merge** new roll data into existing `roll_call_votes`, then **UpdateItem** the bill (or write oversize and set key).
6. **SEARCH#VOTE update** (`update_search_vote_index_for_bill`): Uses **in-memory** `roll_call_votes` only. No extra API. For each entry (each roll), for each member:
   - Map `voteCast` → bucket (yea/nea/present/not_voting).
   - Resolve member → politician_id (bioguide or NAME#…).
   - Add this bill_id to that politician’s `bill_yea` / `bill_nea` / etc.
   - Add this roll_id to that politician’s `roll_yea` / `roll_nea` / etc.
7. **BatchGetItem** existing SEARCH#VOTE items for those politicians, **merge** the new ids into the lists, **BatchWriteItem** back.

So we are **not** “calling the API for each roll” when building the vote index. We call the API only for **new** rolls (delta). The vote-index step only iterates the `roll_call_votes` we already have and updates DynamoDB.

### Second pass (house-vote list)

- **Query** SEARCH#ROLL for existing (congress, session, roll) keys (requires `dynamodb:Query`).
- For each roll in the house-vote list that is **not** in “from bills” and **not** in “already in table”, **one API call** to get members, then write SEARCH#ROLL and update SEARCH#VOTE (`update_search_vote_index_for_roll`), again using in-memory members only.

---

## 2. Why we map votes into columns (yea/nea/present/not_voting)

**Design goal:** For “show me politician X’s votes,” the client should get **one response** that includes every vote (bill + roll + how they voted) and be able to filter by vote type (Yea/Nay/Present/Not Voting) on the client.

**Current design:**

- SEARCH#VOTE item per politician has 8 lists: `bill_yea`, `bill_nea`, `bill_present`, `bill_not_voting`, `roll_yea`, `roll_nea`, `roll_present`, `roll_not_voting`.
- Backfill maps each member’s `voteCast` (Aye/Yea/Nay/No/Present/Not Voting) into one of these buckets and appends bill_id and roll_id to the right lists.
- API: one **BatchGetItem** for the requested politician_ids returns those 8 lists. We then attach bill_details and roll_dates and return. The frontend **flattens** the 8 lists into one row per (politician, roll, voteType) and **filters by vote type on the client** (e.g. “Yea only”).

**Alternative: “just bill and roll” (no vote-type columns)**

- If we stored only “all bill_ids this member voted on” and “all roll_ids this member voted on” with **no** yea/nea/present/not_voting split, then to show “only Yea votes” we would have to know **how** they voted on each roll.
- That would require loading **each roll’s full members list** (GetItem SEARCH#ROLL or roll_call_details) and scanning for this politician’s `voteCast`. So: 1 call for SEARCH#VOTE + **N** calls for N rolls (or one big batch of roll details). That is **N+1** round-trips and more latency/cost.
- So we **do** need vote type attached to each bill/roll for “single call + client-side filter.” The only choice is **where** we attach it:
  - **Current:** Pre-attach at backfill time by writing into 8 lists (bill_yea, bill_nea, …). One read returns everything; client filters by column.
  - **Alternative shape:** Store one list of `{ bill_id, vote_type }` and one of `{ roll_id, vote_type }`. Same information, still requires mapping at backfill time; client still filters by vote_type. DynamoDB would store more complex list items instead of simple string lists.

So **mapping voteCast into columns (or equivalent) is required** to keep “fetch all votes for this politician in one call and filter by vote type on the client.” Doing “bill and roll only” and “handling vote filtering on the client side” would only work if we **also** had vote type per bill/roll—which is exactly what the 8 columns (or a (id, vote_type) structure) provide. We already fetch all votes and bills with a single call; the columns are what make client-side vote-type filtering possible without extra round-trips.

---

## 3. Alternative: store full vote structure (no 8-column parse)

**What you mean:** Instead of parsing each vote into `bill_yea` / `roll_yea` / etc., store the **entire data structure** for each vote in a single list. Each entry would be one object per (bill, roll, vote), e.g.:

```json
vote_entries: [
  { "bill_id": "119-HR-1286", "roll_id": "119#2#70", "vote_type": "Yea" },
  { "bill_id": "119-HR-1286", "roll_id": "119#2#71", "vote_type": "Nay" },
  ...
]
```

- **Backfill:** No 8-way bucket logic, no normalization. The API already returns normalized vote values. For each member we already have: **append one object** `{ bill_id, roll_id, vote_type }` (or whatever the API field is) and store as-is. We just persist and display it.
- **API:** Same one BatchGetItem; return the list of vote objects (plus bill_details / roll_dates if needed). Client still gets one call and **filters by vote_type on the client** (e.g. filter `vote_entries` where `vote_type === 'Yea'`).
- **Outcome:** Same "single call + client-side filter by vote type." Simpler backfill (no maintaining 8 parallel lists and no index alignment).

**Tradeoffs:**

| Aspect | 8 columns (current) | Single list of full structures |
|--------|---------------------|---------------------------------|
| Backfill | Map voteCast → bucket, append to 8 lists, keep bill_* and roll_* aligned by index | Normalize voteCast → vote_type, append one object per vote |
| DynamoDB size | 8 list attributes; each element is a short string (bill_id or roll_id) | 1 list attribute; each element is a map (bill_id, roll_id, vote_type) — larger per vote |
| Oversize | 8 lists can be moved to S3 together; frontend assumes roll_yea[i] ↔ bill_yea[i] | One list; oversize when total vote_entries size exceeds 400KB; same S3 pattern |
| Client | Flatten 8 lists into rows, filter by voteType | Use list as-is, filter by vote_type |

So **yes**, storing the full vote structure in one list (and not parsing into bill_yea/roll_yea/etc.) is a valid alternative: simpler backfill, same one-call + client-side filtering. **No normalization anywhere** — the API sends normalized data; we store it and display it. The main cost is slightly larger items (one map per vote). If we ever refactor SEARCH#VOTE, this shape is a good candidate.

---

## 4. Summary

| Question | Answer |
|----------|--------|
| Do we call the API for each roll when updating the vote index? | No. We call the API only for **new** rolls (delta). The vote-index update uses in-memory `roll_call_votes` / members. |
| Are we going through each roll and mapping votes into columns? | Yes. For each roll we already have, we iterate its members, map `voteCast` → bucket, and add bill_id/roll_id to that politician’s bill_yea/roll_yea (or nea/present/not_voting). |
| Would “just bill and roll” + client-side filtering be better? | No. Without storing vote type per bill/roll, client cannot filter by Yea/Nay/Present/Not Voting without fetching every roll’s members (N+1 calls). Storing vote type (current 8 columns or a (id, vote_type) shape) is what allows single-call fetch + client-side vote-type filter. |
