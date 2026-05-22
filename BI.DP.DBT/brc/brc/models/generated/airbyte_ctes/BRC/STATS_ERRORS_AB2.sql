{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(STER_BACKUP_DATE as {{ dbt_utils.type_string() }}) as STER_BACKUP_DATE,
	try_cast(STER_CHECKED_TIME as {{ dbt_utils.type_string() }}) as STER_CHECKED_TIME,
	try_cast(STER_FK_ADVERTISER as {{ dbt_utils.type_float() }}) as STER_FK_ADVERTISER,
	try_cast(STER_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as STER_FK_PUBLISHER,
	try_cast(STER_ID as {{ dbt_utils.type_float() }}) as STER_ID,
	try_cast(STER_MISSING_JSON as {{ dbt_utils.type_string() }}) as STER_MISSING_JSON,
	try_cast(STER_MONTH as {{ dbt_utils.type_string() }}) as STER_MONTH,
	try_cast(STER_STATUS as {{ dbt_utils.type_string() }}) as STER_STATUS,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('STATS_ERRORS_AB1') }}
-- STATS_ERRORS
where 1 = 1