{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(POST_3RD_PARTY_CLICKID as {{ dbt_utils.type_string() }}) as POST_3RD_PARTY_CLICKID,
	try_cast(POST_CLICK_HTTPCODE as {{ dbt_utils.type_float() }}) as POST_CLICK_HTTPCODE,
	try_cast(POST_CLICK_TIMESTAMP as {{ dbt_utils.type_string() }}) as POST_CLICK_TIMESTAMP,
	try_cast(POST_CLICK_URL as {{ dbt_utils.type_string() }}) as POST_CLICK_URL,
	try_cast(POST_CLICKID as {{ dbt_utils.type_string() }}) as POST_CLICKID,
	try_cast(POST_FK_CAMT_ID as {{ dbt_utils.type_float() }}) as POST_FK_CAMT_ID,
	try_cast(POST_FK_TRACKER as {{ dbt_utils.type_float() }}) as POST_FK_TRACKER,
	try_cast(POST_ID as {{ dbt_utils.type_float() }}) as POST_ID,
	try_cast(POST_SUBID as {{ dbt_utils.type_string() }}) as POST_SUBID,
	try_cast(POST_SUBID_2 as {{ dbt_utils.type_string() }}) as POST_SUBID_2,
	try_cast(POST_SUBID_3 as {{ dbt_utils.type_string() }}) as POST_SUBID_3,
	try_cast(POST_SUBID_4 as {{ dbt_utils.type_string() }}) as POST_SUBID_4,
	try_cast(POST_SUBID_5 as {{ dbt_utils.type_string() }}) as POST_SUBID_5,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	S3_PATH,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_3RD_PARTY_CLICK_LOG_AB1') }}
-- POSTBACK_3RD_PARTY_CLICK_LOG
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}