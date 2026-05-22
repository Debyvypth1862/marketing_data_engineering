{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['nese_fk_news'], ['nese_fk_news']) }} as NESE_FK_NEWS,
	{{ json_extract_scalar('_airbyte_data', ['nese_fk_publisher'], ['nese_fk_publisher']) }} as NESE_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['nese_id'], ['nese_id']) }} as NESE_ID,
	{{ json_extract_scalar('_airbyte_data', ['nese_seen'], ['nese_seen']) }} as NESE_SEEN,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_PUBLISHER_NEWS_SEEN') }} as table_alias
where 1 = 1
