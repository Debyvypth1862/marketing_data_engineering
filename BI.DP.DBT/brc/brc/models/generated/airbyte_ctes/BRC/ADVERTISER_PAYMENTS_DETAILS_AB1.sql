{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['adde_created'], ['adde_created']) }} as ADDE_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['adde_fk_adpa_id'], ['adde_fk_adpa_id']) }} as ADDE_FK_ADPA_ID,
	{{ json_extract_scalar('_airbyte_data', ['adde_fk_advertiser'], ['adde_fk_advertiser']) }} as ADDE_FK_ADVERTISER,
	{{ json_extract_scalar('_airbyte_data', ['adde_fk_payer'], ['adde_fk_payer']) }} as ADDE_FK_PAYER,
	{{ json_extract_scalar('_airbyte_data', ['adde_id'], ['adde_id']) }} as ADDE_ID,
	{{ json_extract_scalar('_airbyte_data', ['adde_income'], ['adde_income']) }} as ADDE_INCOME,
	{{ json_extract_scalar('_airbyte_data', ['adde_updated'], ['adde_updated']) }} as ADDE_UPDATED,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,

	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISER_PAYMENTS_DETAILS') }} as table_alias
-- ADVERTISER_PAYMENTS_DETAILS
where 1 = 1
