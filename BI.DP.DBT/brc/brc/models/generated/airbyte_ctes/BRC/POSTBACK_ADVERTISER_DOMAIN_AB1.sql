{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['poad_fk_advertiser'], ['poad_fk_advertiser']) }} as POAD_FK_ADVERTISER,
	{{ json_extract_scalar('_airbyte_data', ['poad_fk_postback_domain'], ['poad_fk_postback_domain']) }} as POAD_FK_POSTBACK_DOMAIN,
	{{ json_extract_scalar('_airbyte_data', ['poad_id'], ['poad_id']) }} as POAD_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_ADVERTISER_DOMAIN') }} as table_alias
where 1 = 1
