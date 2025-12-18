# LDA API Autocomplete & Dropdown Endpoints Analysis

## Summary

This document analyzes all LDA API endpoints to identify which ones can be used for:
1. **Autocomplete** - Searchable endpoints that support name-based queries
2. **Dropdowns** - Static constant lists for form dropdowns

---

## Autocomplete Endpoints (Searchable by Name)

**✅ ALL THREE ENDPOINTS SUPPORT PARTIAL MATCHING (AUTOCOMPLETE)!**

These endpoints support partial text search and can be used for autocomplete functionality:

### 1. Registrants (`/api/v1/registrants/`)
- **Endpoint**: `GET /api/v1/registrants/`
- **Search Parameter**: `registrant_name` (string)
- **Total Count**: ~17,052 registrants
- **Partial Matching**: ✅ **YES** - Supports partial name matching
  - Example: `registrant_name=ACME` matches "ACME PUBLIC AFFAIRS, LLC"
  - Example: `registrant_name=Microsoft` matches "MICROSOFT CORPORATION"
  - Example: `registrant_name=VAN SCOYOC` matches "VAN SCOYOC ASSOCIATES" and "VAN SCOYOC KELLY"
- **Use Case**: Autocomplete for registrant name/ID search
- **Response Fields**:
  - `id` - Unique ID
  - `name` - Registrant name
  - `house_registrant_id` - House registrant ID
  - `description` - Description
  - Address fields, country, state, etc.
- **Query Example**: `?registrant_name=ACME&page_size=10`

### 2. Clients (`/api/v1/clients/`)
- **Endpoint**: `GET /api/v1/clients/`
- **Search Parameter**: `client_name` (string)
- **Total Count**: ~131,997 clients
- **Partial Matching**: ✅ **YES** - Supports partial name matching
  - Example: `client_name=Microsoft` returns 111 results (all containing "Microsoft")
  - Example: `client_name=AMERICAN` returns 5,414 results
  - Example: `client_name=BLUECROSS` returns 15 results
- **Use Case**: Autocomplete for client name/ID search
- **Response Fields**:
  - `id` - Unique ID
  - `client_id` - Client ID (string)
  - `name` - Client name
  - `general_description` - Description
  - Country, state, PPB fields, etc.
- **Query Example**: `?client_name=Microsoft&page_size=10`

### 3. Lobbyists (`/api/v1/lobbyists/`)
- **Endpoint**: `GET /api/v1/lobbyists/`
- **Search Parameter**: `lobbyist_name` (string)
- **Total Count**: ~86,806 lobbyists
- **Partial Matching**: ✅ **YES** - Supports partial name matching across name components
  - Example: `lobbyist_name=Smith` returns 679 results (matches last name "SMITH")
  - Example: `lobbyist_name=PARKER` returns 103 results (matches last name "PARKER")
  - Example: `lobbyist_name=KRISTIN` returns 260 results (matches first name "KRISTIN")
  - Example: `lobbyist_name=VAN SCOYOC` returns 6 results (matches last name "VAN SCOYOC")
  - **Note**: Searches across `first_name`, `last_name`, `middle_name` fields
- **Use Case**: Autocomplete for lobbyist name search
- **Response Fields**:
  - `id` - Unique ID
  - `first_name`, `middle_name`, `last_name` - Name components
  - `prefix`, `prefix_display` - Title prefix
  - `suffix`, `suffix_display` - Name suffix
  - `nickname` - Nickname
  - `registrant` - Associated registrant object
- **Query Example**: `?lobbyist_name=Smith&page_size=10`
- **Display Note**: Construct full name from `prefix_display`, `first_name`, `middle_name`, `last_name`, `suffix_display` for display

---

## Dropdown/Constants Endpoints (Static Lists)

These endpoints return static lists perfect for dropdown menus:

### 1. Filing Types (`/api/v1/constants/filing/filingtypes/`)
- **Endpoint**: `GET /api/v1/constants/filing/filingtypes/`
- **Response**: Array of `{name: string, value: string}`
- **Count**: ~50 filing types
- **Use Case**: Dropdown for Report Type filter
- **Examples**: "Registration", "Registration - Amendment", "1st Quarter - Report", etc.

### 2. Lobbying Activity Issues (`/api/v1/constants/filing/lobbyingactivityissues/`)
- **Endpoint**: `GET /api/v1/constants/filing/lobbyingactivityissues/`
- **Response**: Array of `{name: string, value: string}`
- **Count**: ~79 issue areas
- **Use Case**: Dropdown for Issue Area filter
- **Examples**: "Taxation/Internal Revenue Code", "Energy/Nuclear", "Sports/Athletics", etc.

### 3. Government Entities (`/api/v1/constants/filing/governmententities/`)
- **Endpoint**: `GET /api/v1/constants/filing/governmententities/`
- **Response**: Array of `{id: integer, name: string}`
- **Use Case**: Dropdown for "Govt. Entity Contacted" filter
- **Examples**: "HOUSE OF REPRESENTATIVES", "SENATE", "Defense, Dept of (DOD)", etc.

### 4. Countries (`/api/v1/constants/general/countries/`)
- **Endpoint**: `GET /api/v1/constants/general/countries/`
- **Response**: Array of `{name: string, value: string}`
- **Use Case**: Dropdown for country filters (Registrant Country, Client Country, PPB Country, etc.)
- **Examples**: "United States of America" (US), "Canada" (CA), etc.

### 5. States (`/api/v1/constants/general/states/`)
- **Endpoint**: `GET /api/v1/constants/general/states/`
- **Response**: Array of `{name: string, value: string}`
- **Use Case**: Dropdown for state filters (Client State, PPB State, etc.)
- **Examples**: "Alabama" (AL), "California" (CA), etc.

### 6. Lobbyist Prefixes (`/api/v1/constants/lobbyist/prefixes/`)
- **Endpoint**: `GET /api/v1/constants/lobbyist/prefixes/`
- **Response**: Array of `{name: string, value: string}`
- **Use Case**: Dropdown for lobbyist title prefix (if needed)
- **Examples**: "DR.", "MR.", "MRS.", "MS.", etc.

### 7. Lobbyist Suffixes (`/api/v1/constants/lobbyist/suffixes/`)
- **Endpoint**: `GET /api/v1/constants/lobbyist/suffixes/`
- **Response**: Array of `{name: string, value: string}`
- **Use Case**: Dropdown for lobbyist name suffix (if needed)
- **Examples**: "JR.", "SR.", "I", "II", "III", etc.

### 8. Contribution Item Types (`/api/v1/constants/contribution/itemtypes/`)
- **Endpoint**: `GET /api/v1/constants/contribution/itemtypes/`
- **Response**: Array of `{name: string, value: string}`
- **Use Case**: Dropdown for Contribution Type filter (LD-203)
- **Examples**: "FECA", "HE", "ME", "PLE", "PIC"

---

## Recommended Implementation Strategy

### For Autocomplete (Search-as-you-type):

1. **Registrants Autocomplete**
   - Endpoint: `/api/v1/registrants/`
   - Query: `?registrant_name={search_term}&page_size=10`
   - Display: `name` (with `id` and `house_registrant_id` as metadata)
   - Cache: Consider caching frequently searched registrants

2. **Clients Autocomplete**
   - Endpoint: `/api/v1/clients/`
   - Query: `?client_name={search_term}&page_size=10`
   - Display: `name` (with `id` and `client_id` as metadata)
   - Cache: Consider caching frequently searched clients

3. **Lobbyists Autocomplete**
   - Endpoint: `/api/v1/lobbyists/`
   - Query: `?lobbyist_name={search_term}&page_size=10`
   - Display: Construct full name from `prefix_display`, `first_name`, `middle_name`, `last_name`, `suffix_display`
   - Cache: Consider caching frequently searched lobbyists

### For Dropdowns (Load once, cache locally):

1. **Filing Types** - Load on form initialization, cache in frontend
2. **Lobbying Activity Issues** - Load on form initialization, cache in frontend
3. **Government Entities** - Load on form initialization, cache in frontend
4. **Countries** - Load on form initialization, cache in frontend
5. **States** - Load on form initialization, cache in frontend
6. **Contribution Item Types** - Load on LD-203 form initialization, cache in frontend
7. **Lobbyist Prefixes/Suffixes** - Optional, only if needed for name construction

---

## Implementation Notes

### Autocomplete Best Practices:
- **Debounce**: Wait 300-500ms after user stops typing before making API call
- **Page Size**: Use `page_size=10` or `page_size=20` for autocomplete results
- **Rate Limiting**: Implement client-side rate limiting to avoid excessive API calls
- **Caching**: Cache recent search results in browser/localStorage
- **Error Handling**: Handle 429 (rate limit) errors gracefully with retry logic

### Dropdown Best Practices:
- **Load Once**: Fetch constants on form initialization, not on every render
- **Cache**: Store in React state, Redux, or localStorage
- **Refresh Strategy**: Constants rarely change, refresh weekly/monthly if needed
- **Error Handling**: Have fallback values if API fails

### Data Structure Recommendations:

**For Autocomplete Results:**
```typescript
interface AutocompleteOption {
  id: number | string;
  name: string;
  displayName: string; // Formatted for display
  metadata?: {
    house_registrant_id?: number;
    client_id?: string;
    // Other relevant fields
  };
}
```

**For Constants:**
```typescript
interface ConstantOption {
  name: string;  // Display name
  value: string; // API value
  // For government entities:
  id?: number;
}
```

---

## Endpoint Summary Table

| Endpoint | Type | Count | Search Param | Use Case |
|----------|------|-------|--------------|----------|
| `/registrants/` | Autocomplete | ~17K | `registrant_name` | Registrant search |
| `/clients/` | Autocomplete | ~132K | `client_name` | Client search |
| `/lobbyists/` | Autocomplete | ~87K | `lobbyist_name` | Lobbyist search |
| `/constants/filing/filingtypes/` | Dropdown | ~50 | N/A | Report type dropdown |
| `/constants/filing/lobbyingactivityissues/` | Dropdown | ~79 | N/A | Issue area dropdown |
| `/constants/filing/governmententities/` | Dropdown | ~200+ | N/A | Government entity dropdown |
| `/constants/general/countries/` | Dropdown | ~244 | N/A | Country dropdowns |
| `/constants/general/states/` | Dropdown | ~49 | N/A | State dropdowns |
| `/constants/lobbyist/prefixes/` | Dropdown | ~7 | N/A | Prefix dropdown (optional) |
| `/constants/lobbyist/suffixes/` | Dropdown | ~13 | N/A | Suffix dropdown (optional) |
| `/constants/contribution/itemtypes/` | Dropdown | ~5 | N/A | Contribution type dropdown |

---

## Next Steps

1. **Test autocomplete endpoints** with sample queries to verify search behavior
2. **Fetch and cache constants** on application startup
3. **Implement autocomplete components** with debouncing and caching
4. **Create API wrapper functions** for consistent error handling and rate limiting

