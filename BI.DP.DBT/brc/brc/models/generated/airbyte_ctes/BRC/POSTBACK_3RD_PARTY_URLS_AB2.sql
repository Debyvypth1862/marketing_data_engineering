{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(PURL_CLICK_URL as {{ dbt_utils.type_string() }}) as PURL_CLICK_URL,
	try_cast(PURL_CPA_URL as {{ dbt_utils.type_string() }}) as PURL_CPA_URL,
	try_cast(PURL_FK_CAMT_ID as {{ dbt_utils.type_float() }}) as PURL_FK_CAMT_ID,
	try_cast(PURL_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as PURL_FK_PUBLISHER,
	try_cast(PURL_FTD_URL as {{ dbt_utils.type_string() }}) as PURL_FTD_URL,
	try_cast(PURL_ID as {{ dbt_utils.type_float() }}) as PURL_ID,
	try_cast(PURL_SIGNUP_URL as {{ dbt_utils.type_string() }}) as PURL_SIGNUP_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_3RD_PARTY_URLS_AB1') }}
-- POSTBACK_3RD_PARTY_URLS
where 1 = 1