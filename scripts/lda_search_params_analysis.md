# LDA Search Parameters Analysis

## All Search Parameters from LD-1 & LD-2 Search Form

### Registrant
1. **registrant_name** - Name or ID (text search)
2. **registrant_id** - ID lookup
3. **registrant_country** - Country filter
4. **registrant_ppb_country** - Primary Place of Business Country

### Client
5. **client_name** - Name or ID (text search)
6. **client_id** - ID lookup
7. **client_state** - State filter
8. **client_country** - Country filter
9. **client_ppb_country** - Primary Place of Business Country
10. **client_ppb_state** - Primary Place of Business State

### Lobbyist
11. **lobbyist_name** - Name (text search)
12. **lobbyist_id** - ID lookup
13. **lobbyist_covered_position** - Covered Position Description (text search)
14. **lobbyist_covered_position_indicator** - Any Covered Government Position(s) (boolean)
15. **lobbyist_conviction_disclosure** - Conviction Description (text search)
16. **lobbyist_conviction_disclosure_indicator** - Any Disclosed Conviction(s) (boolean)
17. **lobbyist_conviction_date_range_after** - Conviction Date From
18. **lobbyist_conviction_date_range_before** - Conviction Date To

### Report/Filing
19. **filing_type** - Report Type (enum: RR, RA, Q1, Q1Y, 1T, 1TY, etc.)
20. **filing_period** - Filing Period (enum: first_quarter, second_quarter, third_quarter, fourth_quarter, mid_year, year_end)
21. **filing_year** - Filing Year (integer: 2025, 2024, 2023, etc.)
22. **filing_dt_posted_after** - Report Posted From (date)
23. **filing_dt_posted_before** - Report Posted To (date)
24. **filing_amount_reported_min** - Amount Reported Min (float)
25. **filing_amount_reported_max** - Amount Reported Max (float)
26. **senate_doc_id** - Senate Doc ID (text)
27. **house_doc_id** - House Doc ID (text)

### Lobbying Activity
28. **filing_specific_lobbying_issues** - Specific Lobbying Issues (text search)
29. **general_issue_code** - Issue Area (enum: TAX, ENG, SPO, etc.)
30. **government_entities** - Govt. Entity Contacted (array of entity IDs)

### Affiliated Organizations
31. **affiliated_organization_name** - Name (text search)
32. **affiliated_organization_listed_indicator** - Any Affiliated Organizations Listed (boolean)
33. **affiliated_organization_country** - Country filter

### Foreign Entities
34. **foreign_entity_name** - Name (text search)
35. **foreign_entity_listed_indicator** - Any Foreign Entities Listed (boolean)
36. **foreign_entity_country** - Country filter
37. **foreign_entity_ppb_country** - Primary Place of Business Country
38. **foreign_entity_ownership_percentage_min** - Owner Percentage Min (0-100)
39. **foreign_entity_ownership_percentage_max** - Owner Percentage Max (0-100)

---

## Recommended GSIs (High-Value Search Fields)

### Primary Search Patterns (Definitely Index):
1. **filing_year** - Very common filter, range queries
2. **filing_period** - Common filter, categorical
3. **filing_type** - Common filter, categorical
4. **filing_dt_posted** - Date range queries (posted between dates)
5. **filing_amount_reported** - Range queries (amount between min/max)
6. **registrant_name** - Name-based search (autocomplete returns ID too) ⭐
7. **client_name** - Name-based search (autocomplete returns ID too) ⭐
8. **lobbyist_name** - Name-based search (autocomplete returns ID too) ⭐

### Alternative: ID-based GSIs (if needed for exact lookups)
- **registrant_id** - Direct ID lookups (can get from autocomplete)
- **client_id** - Direct ID lookups (can get from autocomplete)
- **lobbyist_id** - Direct ID lookups (can get from autocomplete)

**Note**: Since autocomplete endpoints return both name and ID, indexing by name is more user-friendly. Users can search by name, and the autocomplete response provides the ID for exact filtering if needed.

### Secondary Search Patterns (Consider Indexing):
9. **registrant_country** - Country filtering
10. **client_country** - Country filtering
11. **client_state** - State filtering
12. **registrant_ppb_country** - PPB country filtering
13. **client_ppb_country** - PPB country filtering
14. **general_issue_code** - Issue area filtering (if frequently used)

### Not Recommended for GSI (Use Full-Text Search Instead):
- **filing_specific_lobbying_issues** - Text search, free-form text
- **lobbyist_covered_position** - Text search, free-form text
- **lobbyist_conviction_disclosure** - Text search, free-form text
- **affiliated_organization_name** - Text search, free-form text
- **foreign_entity_name** - Text search, free-form text
- **senate_doc_id** - Rare, specific lookup (can use exact match if needed)
- **house_doc_id** - Rare, specific lookup (can use exact match if needed)
- Boolean flags (covered_position_indicator, conviction_disclosure_indicator, etc.) - Can be filtered in query

**Note**: `registrant_name`, `client_name`, and `lobbyist_name` are now recommended for GSI indexing since:
- Autocomplete endpoints support partial matching
- Users search by name, not ID
- Autocomplete responses include both name and ID for exact lookups
- Name-based GSIs provide better UX than ID-based GSIs

---

## Recommended GSI Strategy

### High Priority GSIs (8 indexes):
1. `filing_year` (Number) - Range queries
2. `filing_period` (String) - Categorical filter
3. `filing_type` (String) - Categorical filter
4. `filing_dt_posted` (String/Number) - Date range queries
5. `filing_amount_reported` (Number) - Range queries
6. `registrant_name` (String) - Name-based search (user-friendly, autocomplete provides ID)
7. `client_name` (String) - Name-based search (user-friendly, autocomplete provides ID)
8. `lobbyist_name` (String) - Name-based search (user-friendly, autocomplete provides ID)

### Why Name Instead of ID?
- **User Experience**: Users search by name, not ID
- **Autocomplete Integration**: Autocomplete endpoints return both name and ID
- **Flexibility**: Can still do ID-based exact lookups using the ID from autocomplete results
- **Search Patterns**: Name-based searches are more common than ID lookups

### Medium Priority GSIs (5 indexes - if needed):
9. `registrant_country` (String) - Country filter
10. `client_country` (String) - Country filter
11. `client_state` (String) - State filter
12. `registrant_ppb_country` (String) - PPB country filter
13. `client_ppb_country` (String) - PPB country filter

### Total: 8-13 GSIs depending on usage patterns

---

## Notes:
- Text search fields (names, descriptions) should use full-text search capabilities rather than GSIs
- Boolean flags can be filtered in the query without indexing
- Date ranges and numeric ranges benefit significantly from GSIs
- Categorical fields (enums) are good candidates for GSIs if frequently filtered
- Consider composite GSIs for common filter combinations (e.g., filing_year + filing_period)

---

# LD-203 (Contribution Reports) Search Parameters Analysis

## All Search Parameters from LD-203 Search Form

### Filer
1. **registrant_name** - Registrant Name or ID (text search)
2. **registrant_id** - ID lookup
3. **lobbyist_name** - Lobbyist Name (text search)
4. **lobbyist_id** - ID lookup
5. **exclude_registrant_lobbyists** - Exclude reports filed by registrant's lobbyists (boolean flag)

### Report
6. **filing_type** - Report Type (enum: MM, MA, YY, YA - Mid-Year Report, Mid-Year Amendment, Year-End Report, Year-End Amendment)
7. **filing_period** - Filing Period (enum: mid_year, year_end)
8. **filing_year** - Filing Year (integer: 2025, 2024, 2023, etc.)
9. **filing_dt_posted_after** - Report Posted From (date)
10. **filing_dt_posted_before** - Report Posted To (date)
11. **senate_doc_id** - Senate Doc ID (text)
12. **house_doc_id** - House Doc ID (text)

### Contribution Items
13. **contribution_date_after** - Contribution Made From (date)
14. **contribution_date_before** - Contribution Made To (date)
15. **contribution_amount_min** - Contribution Amount Min (float)
16. **contribution_amount_max** - Contribution Amount Max (float)
17. **contribution_type** - Contribution Type (enum - categorical)
18. **contributor_name** - Contributor Name (text search)
19. **payee_name** - Payee Name (text search)
20. **honoree_name** - Honoree Name (text search)

---

## Recommended GSIs for LD-203 (High-Value Search Fields)

### Primary Search Patterns (Definitely Index):
1. **filing_year** - Very common filter, range queries
2. **filing_period** - Common filter, categorical
3. **filing_type** - Common filter, categorical (MM, MA, YY, YA)
4. **filing_dt_posted** - Date range queries (posted between dates)
5. **registrant_id** - Direct ID lookups
6. **lobbyist_id** - Direct ID lookups
7. **contribution_date** - Contribution date range queries (if stored at report level or aggregated)
8. **contribution_amount** - Contribution amount range queries (if stored at report level or aggregated)
9. **contribution_type** - Contribution type filtering (categorical)

### Not Recommended for GSI (Use Full-Text Search Instead):
- **registrant_name** - Text search, better with full-text index
- **lobbyist_name** - Text search, better with full-text index
- **contributor_name** - Text search (within contribution_items array)
- **payee_name** - Text search (within contribution_items array)
- **honoree_name** - Text search (within contribution_items array)
- **senate_doc_id** - Rare, specific lookup
- **house_doc_id** - Rare, specific lookup
- **exclude_registrant_lobbyists** - Boolean flag, can be filtered in query

---

## Recommended GSI Strategy for LD-203

### High Priority GSIs (9 indexes):
1. `filing_year` (Number) - Range queries
2. `filing_period` (String) - Categorical filter
3. `filing_type` (String) - Categorical filter (MM, MA, YY, YA)
4. `filing_dt_posted` (String/Number) - Date range queries
5. `registrant_id` (Number) - Direct lookups
6. `lobbyist_id` (Number) - Direct lookups
7. `contribution_date` (String/Number) - Contribution date range (if applicable)
8. `contribution_amount` (Number) - Contribution amount range (if applicable)
9. `contribution_type` (String) - Contribution type filter (if applicable)

### Notes on Contribution Fields:
- **contribution_date** and **contribution_amount** are within the `contribution_items` array
- These may need to be:
  - Stored as aggregated values at the report level (min/max dates, min/max amounts)
  - OR queried using array/list operations if DynamoDB supports it
  - OR require a separate table/index for contribution items if they need to be searched independently

### Total: 9 GSIs for LD-203

---

## Combined GSI Strategy (LD-1/LD-2 + LD-203)

### Shared GSIs (used by both):
1. `filing_year` (Number)
2. `filing_period` (String)
3. `filing_type` (String)
4. `filing_dt_posted` (String/Number)
5. `registrant_name` (String) - Name-based (autocomplete provides ID)
6. `lobbyist_name` (String) - Name-based (autocomplete provides ID)

### LD-1/LD-2 Specific GSIs:
7. `client_name` (String) - Name-based (autocomplete provides ID)
8. `filing_amount_reported` (Number)
9. `registrant_country` (String) - Optional
10. `client_country` (String) - Optional
11. `client_state` (String) - Optional

### LD-203 Specific GSIs:
12. `contribution_date` (String/Number) - If applicable
13. `contribution_amount` (Number) - If applicable
14. `contribution_type` (String) - If applicable

### Total Combined: 6 shared + 2 LD-1/LD-2 specific + 3 LD-203 specific = 11 core GSIs
### With optional: +3 more = 14 total GSIs maximum

### Name vs ID Indexing Strategy:
- **Index by Name**: More user-friendly, aligns with autocomplete UX
- **ID Available**: Autocomplete responses include ID, so exact ID lookups are still possible
- **Search Flow**: User types name → Autocomplete shows options with IDs → User selects → System uses ID for exact filtering
- **Fallback**: If needed, can add ID-based GSIs later, but name-based is preferred for UX

---

## Final Recommendations:
- **Start with 6 shared GSIs** that work for both filing types
- **Add filing-specific GSIs** based on actual usage patterns
- **Text search fields** should use full-text search (Elasticsearch, OpenSearch, or DynamoDB Streams + search service)
- **Contribution item fields** may require separate consideration depending on data model (array vs separate table)

