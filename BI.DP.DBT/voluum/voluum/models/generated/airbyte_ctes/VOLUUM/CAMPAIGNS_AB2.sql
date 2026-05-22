{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}
select
    try_cast(ALLOWED_ACTIONS as {{ dbt.type_string() }}) as ALLOWED_ACTIONS,
	try_cast(BASIC as {{ dbt.type_boolean() }}) as BASIC,
	try_cast(COST_MODEL as {{ dbt.type_string() }}) as COST_MODEL,
	try_cast(COUNTRY as {{ dbt.type_string() }}) as COUNTRY,
	TO_VARCHAR(CONVERT_TIMEZONE('CET', 'America/Los_Angeles', TRY_TO_TIMESTAMP(CREATED_TIME,'YYYY-MM-DDTHH24:MI:SS.FF3Z')),'YYYY-MM-DD HH12:MI:SS AM') AS CREATED_TIME,
	try_cast(CUSTOM_POSTBACKS_CONFIGURATION as {{ dbt.type_string() }}) as CUSTOM_POSTBACKS_CONFIGURATION,
	try_cast(DELETED as {{ dbt.type_boolean() }}) as DELETED,
	try_cast(DIRECT_TRACKING as {{ dbt.type_boolean() }}) as DIRECT_TRACKING,
	try_cast(DIRECT_TRACKING_LANDER_ID as {{ dbt.type_string() }}) as DIRECT_TRACKING_LANDER_ID,
	try_cast(DIRECT_TRACKING_OFFER_ID as {{ dbt.type_string() }}) as DIRECT_TRACKING_OFFER_ID,
	try_cast(ID as {{ dbt.type_string() }}) as ID,
	try_cast(IMPRESSION_URL as {{ dbt.type_string() }}) as IMPRESSION_URL,
	try_cast(NAME as {{ dbt.type_string() }}) as NAME,
	try_cast(NAME_POSTFIX as {{ dbt.type_string() }}) as NAME_POSTFIX,
	try_cast(PREFERRED_TRACKING_DOMAIN as {{ dbt.type_string() }}) as PREFERRED_TRACKING_DOMAIN,
	try_cast(REDIRECT_TARGET as {{ dbt.type_string() }}) as REDIRECT_TARGET,
	try_cast(REVENUE_MODEL as {{ dbt.type_string() }}) as REVENUE_MODEL,
	try_cast(TAGS as {{ dbt.type_string() }}) as TAGS,
	try_cast(TRAFFIC_SOURCE as {{ dbt.type_string() }}) as TRAFFIC_SOURCE,
	try_cast(TRAFFIC_TYPE as {{ dbt.type_string() }}) as TRAFFIC_TYPE,
	TO_VARCHAR(CONVERT_TIMEZONE('CET', 'America/Los_Angeles', TRY_TO_TIMESTAMP(UPDATED_TIME,'YYYY-MM-DDTHH24:MI:SS.FF3Z')),'YYYY-MM-DD HH12:MI:SS AM') AS UPDATED_TIME,
	try_cast(URL as {{ dbt.type_string() }}) as URL,
	try_cast(WORKSPACE as {{ dbt.type_string() }}) as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGNS_AB1') }}
where 1 = 1