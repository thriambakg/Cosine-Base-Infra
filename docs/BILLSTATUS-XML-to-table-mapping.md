# Bill Status XML → Congress Bills Table Mapping

Reference: BILLSTATUS XML User Guide (govinfo bulk data). This doc lists **all XML elements** in the Bill Status bulk data and how each is **stored in the DynamoDB congress-bills table** so we can index/transform without re-enhancing the fetcher.

---

## XML structure (under `<billStatus>` root)

- **`<bill>`** – parent container for the measure. All below are under `<bill>` unless noted.

---

## 1. Bill-level scalars

| XML element | Description | Table field(s) |
|-------------|-------------|----------------|
| `<congress>` | Congress number (e.g. 114) | `congress` (N) |
| `<type>` / `<billType>` | Bill type (H, S, HRES, …) | `bill_type` (S) |
| `<number>` / `<billNumber>` | Bill number | `bill_number` (N), `bill_id` = `{congress}-{type}-{number}` |
| `<introducedDate>` | Introduction date | `introduced_date` (S) |
| `<createDate>` | Record created in Congress.gov | `xml_create_date` (S) |
| `<updateDate>` | Record metadata updated | `xml_update_date` (S), `update_date` (S) — both set from XML |
| `<version>` | Data format version from LOC | `xml_version` (S) |
| `<originChamber>` | House or Senate | `origin_chamber` (S) |
| `<isByRequest>` | “By request” designation | `is_by_request` (S) |

---

## 2. Title

| XML | Description | Table |
|-----|-------------|--------|
| `<title>` | Legacy single title | Fallback for `bill_title` |
| `<titles>` / item | chamberCode, chamberName, parentTitleType, titleType, title | `bill_title` (Display Title or Official as Introduced), `titles_json` (all items) |

---

## 3. Last action snapshot

| XML | Description | Table |
|-----|-------------|--------|
| `<lastAction>` | actionDate, text, links (name, url) | `last_action_json` (S, JSON object) |

---

## 4. Actions

| XML | Description | Table |
|-----|-------------|--------|
| `<actions>` (container) | actionByCounts, actionTypeCounts | `actions_action_by_counts_json`, `actions_action_type_counts_json` |
| `<actions>` / item | actionCode, actionDate, actionTime, committee (name, systemCode), links (link: name, url), sourceSystem (code, name), text, type; per-action recordedVotes | `actions_json` (full list); `action_count`; `actions_summary` (first 10); `latest_action_text`, `latest_action_type` (first item); `recorded_votes_json` (merged from bill + items) |

---

## 5. Recorded votes

| XML | Description | Table |
|-----|-------------|--------|
| `<recordedVotes>` (bill-level) | recordedVote: chamber, congress, date, fullActionName, rollNumber, sessionNumber, url | Merged into `recorded_votes_json`; `has_roll_call` (N) = 1 if any |
| Per-action `<recordedVotes>` | Same (current format) | Merged into `recorded_votes_json` |

---

## 6. Sponsors

| XML | Description | Table |
|-----|-------------|--------|
| `<sponsors>` / item | firstName, middleName, lastName, identifiers (bioguideId, gpoId, lisID), party, state, district | `sponsor_bioguide_id`, `sponsor_full_name`, `sponsor_first_name`, `sponsor_last_name`, `sponsor_party`, `sponsor_state`, `sponsor_district`, `sponsor_url`; identifiers also in sponsor object in code (gpoId, lisID) |

---

## 7. Cosponsors

| XML | Description | Table |
|-----|-------------|--------|
| `<cosponsors>` / item | firstName, middleName, lastName, isOriginalCosponsor, sponsorshipDate, sponsorshipWithdrawnDate, identifiers (bioguideId, gpoId, lisID) | `cosponsors_json` (array, includes gpoId/lisID when present); `cosponsor_count`; `cosponsor_parties` (pipe-separated) |

---

## 8. Summaries

| XML | Description | Table |
|-----|-------------|--------|
| `<summaries>` / billSummaries/item or summary | actionDate, actionDesc, text, versionCode, name, updateDate, lastSummaryUpdateDate | `summaries_json`; `summary_count`; `summary_text` (first 3 truncated) |

---

## 9. Subjects / policy area

| XML | Description | Table |
|-----|-------------|--------|
| `<subjects>` / billSubjects / primarySubjects, otherSubjects | name, parentSubject/name; policy area | `policy_area` (S); `subjects_json` (all); `legislative_subjects` (pipe-separated names, truncated) |
| `<policyArea>` (direct under bill) | name | Same `policy_area` fallback |

---

## 10. Amendments

| XML | Description | Table |
|-----|-------------|--------|
| `<amendments>` / amendment | number, description, purpose, type, latestAction (actionDate, text), amendedBill (congress, number, originChamber, …) | `amendments_json`; `amendment_count` |

---

## 11. Text versions

| XML | Description | Table |
|-----|-------------|--------|
| `<textVersions>` / item | date, type, formats/item (url, type) | `text_versions_json` (each item: type, date, formats[] with url and optional type; first url also at top level for SQS) |
| (backfill / Lambda) | Stored HTML in S3 | `bill_texts` (L): array of `{ name, s3_key, type }`; empty `[]` when none. Fetcher sets `[]`; backfill/Lambda fill from API. Legacy: `bill_text_html_s3_key` (removed on write). |

---

## 12. Calendar numbers

| XML | Description | Table |
|-----|-------------|--------|
| `<calendarNumbers>` / item | calendar, number | `calendar_numbers_json` |

---

## 13. CBO cost estimates

| XML | Description | Table |
|-----|-------------|--------|
| `<cboCostEstimates>` / item | rptPubDate, rptTitle, rptUrl | `cbo_cost_estimates_json` |

---

## 14. Constitutional authority statement

| XML | Description | Table |
|-----|-------------|--------|
| `<constitutionalAuthorityStatementText>` | CDATA text | `constitutional_authority_statement_text` (truncated 50k) |

---

## 15. Committee reports

| XML | Description | Table |
|-----|-------------|--------|
| `<committeeReports>` / committeeReport | citation | `committee_reports_json` |

---

## 16. Committees

| XML | Description | Table |
|-----|-------------|--------|
| `<committees>` / billCommittees / item | chamber, name, systemCode, type, activities (date, name, reports), subcommittees (name, systemCode) | `committees_json` |

---

## 17. Laws

| XML | Description | Table |
|-----|-------------|--------|
| `<laws>` / item | number, type (Public/Private Law) | `laws_json` |

---

## 18. Notes

| XML | Description | Table |
|-----|-------------|--------|
| `<notes>` / Item or item | text (CDATA), links (name, url) | `notes_json` (truncated 30k per text) |

---

## 19. Related bills

| XML | Description | Table |
|-----|-------------|--------|
| `<relatedBills>` / item | congress, number, type, latestTitle, latestAction (actionDate, text), relationshipDetails (identifiedBy, type) | `related_bills_json` |

---

## 20. Dublin Core (root level)

| XML | Description | Table |
|-----|-------------|--------|
| `<dublinCore>` (under billStatus root) | dc:format, dc:language, dc:rights, dc:contributor, dc:description | `dublin_core_json` (object keyed by local name) |

---

## 21. Computed / internal

| Source | Table field |
|--------|-------------|
| Derived | `bill_id`, `search_index_sk`, `bill_url` |
| Derived | `bipartisan` (N 0/1) from sponsor + cosponsor parties |
| Fetcher | `indexed_at`, `last_updated`, `data_source` |
| Backfill (crawler) | `roll_call_number`, `roll_call_votes` (member-level), `bill_text_s3_key` |

---

## DynamoDB key schema (Terraform)

- **PK:** `bill_id` (S)  
- **SK:** `search_index_sk` (S)  
- **GSIs:** SponsorPartyDateIndex, SponsorNameDateIndex, CongressBillTypeIndex, BillTypeDateIndex, BillTitleDateIndex, BillNumberDateIndex, BipartisanDateIndex, LatestActionDateIndex, PolicyAreaDateIndex, HasRollCallIndex, IntroducedDateIndex  

Attributes declared in Terraform: bill_id, search_index_sk, sponsor_full_name, sponsor_party, introduced_date, latest_action_date, congress, bill_type, bill_title, bill_number, bipartisan, policy_area, has_roll_call. All other fields above are stored as item attributes (DynamoDB allows arbitrary attributes).
