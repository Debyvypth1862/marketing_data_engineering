{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(POTV_CLICKID as {{ dbt_utils.type_float() }}) as POTV_CLICKID,
	try_cast(POTV_DATE as {{ dbt_utils.type_string() }}) as POTV_DATE,
	try_cast(POTV_DEPOSIT_VALUE as {{ dbt_utils.type_float() }}) as POTV_DEPOSIT_VALUE,
	try_cast(POTV_ID as {{ dbt_utils.type_float() }}) as POTV_ID,
	try_cast(POTV_REVSHARE as {{ dbt_utils.type_float() }}) as POTV_REVSHARE,
	try_cast(POTV_TIMESTAMP as {{ dbt_utils.type_string() }}) as POTV_TIMESTAMP,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_TRACKING_VALUES_AB1') }}
-- POSTBACK_TRACKING_VALUES
where 1 = 1