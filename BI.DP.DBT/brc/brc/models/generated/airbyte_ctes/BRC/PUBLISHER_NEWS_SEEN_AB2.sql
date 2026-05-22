{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(NESE_FK_NEWS as {{ dbt_utils.type_float() }}) as NESE_FK_NEWS,
	try_cast(NESE_FK_PUBLISHER as {{ dbt_utils.type_float() }}) as NESE_FK_PUBLISHER,
	try_cast(NESE_ID as {{ dbt_utils.type_float() }}) as NESE_ID,
	try_cast(NESE_SEEN as {{ dbt_utils.type_string() }}) as NESE_SEEN,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PUBLISHER_NEWS_SEEN_AB1') }}
-- PUBLISHER_NEWS_SEEN
where 1 = 1
