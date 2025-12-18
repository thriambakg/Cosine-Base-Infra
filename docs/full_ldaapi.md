Registrants
Access Registrant information.

listRegistrants

get
/api/v1/registrants/
Returns all registrants matching the provided filters.

Authorizations:
ApiKeyAuth
query Parameters
country	
any
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 244 more
Country

dt_updated_after	
string <date-time>
Date Update Range (Before / After): yyyy-mm-dd

dt_updated_before	
string <date-time>
Date Update Range (Before / After): yyyy-mm-dd

id	
integer
ID

ordering	
string
Which field to use when ordering the results.

page	
integer
A page number within the paginated result set.

page_size	
integer
Number of results to return per page.

ppb_country	
any
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 244 more
PPB Country

registrant_name	
string
Name

state	
any
Enum: "AL" "AK" "AS" "AZ" "AR" "AA" "AE" "AP" "CA" "CO" … 49 more
State

Responses
200
Response Schema: application/json
count	
integer
next	
string or null <uri>
previous	
string or null <uri>
results	
Array of objects (Registrant)
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
{
"count": 123,
"next": "/api/v1/{operation}/?page=2",
"previous": null,
"results": [
{}
]
}
retrieveRegistrant

get
/api/v1/registrants/{id}/
Returns all registrants matching the provided filters.

Authorizations:
ApiKeyAuth
path Parameters
id
required
integer
A unique integer value identifying this Registrant.

Responses
200
Response Schema: application/json
address_1	
string
address_2	
string or null
address_3	
string or null
address_4	
string or null
city	
string or null
contact_name	
string
contact_telephone	
string
country	
string
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 242 more
country_display	
string
description	
string or null
dt_updated	
string <date-time>
house_registrant_id	
integer or null
id	
integer
name	
string
ppb_country	
string
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 242 more
ppb_country_display	
string
state	
string or null
Enum: "AL" "AK" "AS" "AZ" "AR" "AA" "AE" "AP" "CA" "CO" … 49 more
state_display	
string
url	
string <uri>
zip	
string or null
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
{
"id": 0,
"url": "http://example.com",
"house_registrant_id": 0,
"name": "string",
"description": "string",
"address_1": "string",
"address_2": "string",
"address_3": "string",
"address_4": "string",
"city": "string",
"state": "AL",
"state_display": "string",
"zip": "string",
"country": "US",
"country_display": "string",
"ppb_country": "US",
"ppb_country_display": "string",
"contact_name": "string",
"contact_telephone": "string",
"dt_updated": "2019-08-24T14:15:22Z"
}
Clients
Access Client information.

listClients

get
/api/v1/clients/
Returns all clients matching the provided filters.

Authorizations:
ApiKeyAuth
query Parameters
client_country	
any
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 244 more
Client Country

client_name	
string
Client Name

client_ppb_country	
any
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 244 more
Client PPB Country

client_ppb_state	
any
Enum: "AL" "AK" "AS" "AZ" "AR" "AA" "AE" "AP" "CA" "CO" … 49 more
Client PPB State

client_state	
any
Enum: "AL" "AK" "AS" "AZ" "AR" "AA" "AE" "AP" "CA" "CO" … 49 more
Client State

id	
integer
ID

ordering	
string
Which field to use when ordering the results.

page	
integer
A page number within the paginated result set.

page_size	
integer
Number of results to return per page.

registrant_id	
integer
Registrant ID

registrant_name	
string
Registrant Name

Responses
200
Response Schema: application/json
count	
integer
next	
string or null <uri>
previous	
string or null <uri>
results	
Array of objects (Client)
Array 
client_government_entity	
boolean or null
client_id	
string
client_self_select	
boolean or null
country	
string
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 242 more
country_display	
string
effective_date	
string or null <date>
general_description	
string or null
id	
integer
name	
string
ppb_country	
string
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 242 more
ppb_country_display	
string
ppb_state	
string or null
Enum: "AL" "AK" "AZ" "AR" "CA" "CO" "CT" "DE" "DC" "FL" … 54 more
ppb_state_display	
string
registrant	
object
state	
string or null
Enum: "AL" "AK" "AZ" "AR" "CA" "CO" "CT" "DE" "DC" "FL" … 54 more
state_display	
string
url	
string <uri>
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
{
"count": 123,
"next": "/api/v1/{operation}/?page=2",
"previous": null,
"results": [
{}
]
}
retrieveClient

get
/api/v1/clients/{id}/
Returns all clients matching the provided filters.

Authorizations:
ApiKeyAuth
path Parameters
id
required
integer
A unique integer value identifying this Client.

Responses
200
Response Schema: application/json
client_government_entity	
boolean or null
client_id	
string
client_self_select	
boolean or null
country	
string
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 242 more
country_display	
string
effective_date	
string or null <date>
general_description	
string or null
id	
integer
name	
string
ppb_country	
string
Enum: "US" "CA" "" "00" "AF" "AX" "AL" "DZ" "AS" "AD" … 242 more
ppb_country_display	
string
ppb_state	
string or null
Enum: "AL" "AK" "AZ" "AR" "CA" "CO" "CT" "DE" "DC" "FL" … 54 more
ppb_state_display	
string
registrant	
object
state	
string or null
Enum: "AL" "AK" "AZ" "AR" "CA" "CO" "CT" "DE" "DC" "FL" … 54 more
state_display	
string
url	
string <uri>
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
{
"id": 0,
"url": "http://example.com",
"client_id": "string",
"name": "string",
"general_description": "string",
"client_government_entity": true,
"client_self_select": true,
"state": "AL",
"state_display": "string",
"country": "US",
"country_display": "string",
"ppb_state": "AL",
"ppb_state_display": "string",
"ppb_country": "US",
"ppb_country_display": "string",
"effective_date": "2019-08-24",
"registrant": {
"id": 0,
"url": "http://example.com",
"house_registrant_id": 0,
"name": "string",
"description": "string",
"address_1": "string",
"address_2": "string",
"address_3": "string",
"address_4": "string",
"city": "string",
"state": "AL",
"state_display": "string",
"zip": "string",
"country": "US",
"country_display": "string",
"ppb_country": "US",
"ppb_country_display": "string",
"contact_name": "string",
"contact_telephone": "string",
"dt_updated": "2019-08-24T14:15:22Z"
}
}
Lobbyists
Access Lobbyist information.

listLobbyists

get
/api/v1/lobbyists/
Returns all lobbyists matching the provided filters. The ID is a unique integer value identifying this Lobbyist Name as reported by this Registrant.

Authorizations:
ApiKeyAuth
query Parameters
id	
integer
ID

lobbyist_name	
string
Lobbyist Name

ordering	
string
Which field to use when ordering the results.

page	
integer
A page number within the paginated result set.

page_size	
integer
Number of results to return per page.

registrant_id	
integer
Registrant ID

registrant_name	
string
Registrant Name

Responses
200
Response Schema: application/json
count	
integer
next	
string or null <uri>
previous	
string or null <uri>
results	
Array of objects (LobbyistWithRegistrant)
Array 
first_name	
string or null
id	
integer
last_name	
string or null
middle_name	
string or null
nickname	
string or null
prefix	
string or null
Enum: "dr" "mr" "mrs" "ms" "mx" "ptr" "rev"
prefix_display	
string
registrant	
object
suffix	
string or null
Enum: "jr" "sr" "i" "ii" "iii" "iv" "v" "cae" "dvm" "dmd" … 13 more
suffix_display	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
{
"count": 123,
"next": "/api/v1/{operation}/?page=2",
"previous": null,
"results": [
{}
]
}
retrieveLobbyist

get
/api/v1/lobbyists/{id}/
Returns all lobbyists matching the provided filters. The ID is a unique integer value identifying this Lobbyist Name as reported by this Registrant.

Authorizations:
ApiKeyAuth
path Parameters
id
required
integer
A unique integer value identifying this Lobbyist.

Responses
200
Response Schema: application/json
first_name	
string or null
id	
integer
last_name	
string or null
middle_name	
string or null
nickname	
string or null
prefix	
string or null
Enum: "dr" "mr" "mrs" "ms" "mx" "ptr" "rev"
prefix_display	
string
registrant	
object
suffix	
string or null
Enum: "jr" "sr" "i" "ii" "iii" "iv" "v" "cae" "dvm" "dmd" … 13 more
suffix_display	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
{
"id": 0,
"prefix": "dr",
"prefix_display": "string",
"first_name": "string",
"nickname": "string",
"middle_name": "string",
"last_name": "string",
"suffix": "jr",
"suffix_display": "string",
"registrant": {
"id": 0,
"url": "http://example.com",
"house_registrant_id": 0,
"name": "string",
"description": "string",
"address_1": "string",
"address_2": "string",
"address_3": "string",
"address_4": "string",
"city": "string",
"state": "AL",
"state_display": "string",
"zip": "string",
"country": "US",
"country_display": "string",
"ppb_country": "US",
"ppb_country_display": "string",
"contact_name": "string",
"contact_telephone": "string",
"dt_updated": "2019-08-24T14:15:22Z"
}
}
Constants
An assorted list of constants found in the LDA REST API.

listFilingTypes

get
/api/v1/constants/filing/filingtypes/
Returns all FilingTypes.

Authorizations:
ApiKeyAuth
Responses
200
Response Schema: application/json
Array 
name	
string
value	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
[
{
"name": "string",
"value": "string"
}
]
listLobbyingActivityGeneralIssues

get
/api/v1/constants/filing/lobbyingactivityissues/
Returns all LobbyingActivityGeneralIssues.

Authorizations:
ApiKeyAuth
Responses
200
Response Schema: application/json
Array 
name	
string
value	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response Headers
Retry-After	
integer
Expected available in X seconds.

Response Schema: application/json
detail
required
string
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
[
{
"name": "string",
"value": "string"
}
]
listGovernmentEntities

get
/api/v1/constants/filing/governmententities/
Returns all GovernmentEntities.

Authorizations:
ApiKeyAuth
Responses
200
Response Schema: application/json
Array 
id	
integer
name	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
[
{
"id": 0,
"name": "string"
}
]
listCountries

get
/api/v1/constants/general/countries/
Returns all Countries.

Authorizations:
ApiKeyAuth
Responses
200
Response Schema: application/json
Array 
name	
string
value	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
[
{
"name": "string",
"value": "string"
}
]
listStates

get
/api/v1/constants/general/states/
Returns all States.

Authorizations:
ApiKeyAuth
Responses
200
Response Schema: application/json
Array 
name	
string
value	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
[
{
"name": "string",
"value": "string"
}
]
listLobbyistPrefixes

get
/api/v1/constants/lobbyist/prefixes/
Returns all LobbyistPrefixes.

Authorizations:
ApiKeyAuth
Responses
200
Response Schema: application/json
Array 
name	
string
value	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
[
{
"name": "string",
"value": "string"
}
]
listLobbyistSuffixes

get
/api/v1/constants/lobbyist/suffixes/
Returns all LobbyistSuffixes.

Authorizations:
ApiKeyAuth
Responses
200
Response Schema: application/json
Array 
name	
string
value	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
[
{
"name": "string",
"value": "string"
}
]
listContributionItemTypes

get
/api/v1/constants/contribution/itemtypes/
Returns all ContributionItemTypes.

Authorizations:
ApiKeyAuth
Responses
200
Response Schema: application/json
Array 
name	
string
value	
string
400 Bad Request
401 Unauthorized
404 Not Found
405 Method Not Allowed
429 Too Many Requests - Throttled
Response samples
200400401404405429
Content type
application/json

Copy
Expand allCollapse all
[
{
"name": "string",
"value": "string"
}
]