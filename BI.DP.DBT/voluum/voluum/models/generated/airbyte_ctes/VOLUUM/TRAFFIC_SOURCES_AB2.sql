{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(ALLOWED_ACTIONS as {{ dbt.type_string() }}) as ALLOWED_ACTIONS,
	try_cast(CLICK_ID_VARIABLE as {{ dbt.type_string() }}) as CLICK_ID_VARIABLE,
	try_cast(COST_VARIABLE as {{ dbt.type_string() }}) as COST_VARIABLE,
	TO_VARCHAR(CONVERT_TIMEZONE('CET', 'America/Los_Angeles', TRY_TO_TIMESTAMP(CREATED_TIME,'YYYY-MM-DDTHH24:MI:SS.FF3Z')),'YYYY-MM-DD HH12:MI:SS AM') AS CREATED_TIME,
	try_cast(CURRENCY_CODE as {{ dbt.type_string() }}) as CURRENCY_CODE,
	try_cast(CUSTOM_POSTBACKS_CONFIGURATION as {{ dbt.type_string() }}) as CUSTOM_POSTBACKS_CONFIGURATION,
	try_cast(CUSTOM_VARIABLES as {{ dbt.type_string() }}) as CUSTOM_VARIABLES,
	try_cast(DELETED as {{ dbt.type_boolean() }}) as DELETED,
	try_cast(DIRECT_TRACKING as {{ dbt.type_boolean() }}) as DIRECT_TRACKING,
	try_cast(EXTERNAL_IDS as {{ dbt.type_string() }}) as EXTERNAL_IDS,
	try_cast(ID as {{ dbt.type_string() }}) as ID,
	try_cast(IMPRESSION_SPECIFIC as {{ dbt.type_boolean() }}) as IMPRESSION_SPECIFIC,
	try_cast(LIMITED_GEO_TRACKING as {{ dbt.type_boolean() }}) as LIMITED_GEO_TRACKING,
	try_cast(NAME as {{ dbt.type_string() }}) as NAME,
	try_cast(PIXEL_REDIRECT_URL as {{ dbt.type_string() }}) as PIXEL_REDIRECT_URL,
	try_cast(POSTBACK_URL as {{ dbt.type_string() }}) as POSTBACK_URL,
	try_cast(PREDEFINED_TYPE as {{ dbt.type_string() }}) as PREDEFINED_TYPE,
	try_cast(SKIP_SENDING_POSTBACK as {{ dbt.type_boolean() }}) as SKIP_SENDING_POSTBACK,
	try_cast(TEMPLATE_ID as {{ dbt.type_string() }}) as TEMPLATE_ID,
	try_cast(TYPE as {{ dbt.type_string() }}) as TYPE,
	TO_VARCHAR(CONVERT_TIMEZONE('CET', 'America/Los_Angeles', TRY_TO_TIMESTAMP(UPDATED_TIME,'YYYY-MM-DDTHH24:MI:SS.FF3Z')),'YYYY-MM-DD HH12:MI:SS AM') AS UPDATED_TIME,
	try_cast(WORKSPACE as {{ dbt.type_string() }}) as WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('TRAFFIC_SOURCES_AB1') }}
where 1 = 1