{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['purl_click_url'], ['purl_click_url']) }} as PURL_CLICK_URL,
	{{ json_extract_scalar('_airbyte_data', ['purl_cpa_url'], ['purl_cpa_url']) }} as PURL_CPA_URL,
	{{ json_extract_scalar('_airbyte_data', ['purl_fk_camt_id'], ['purl_fk_camt_id']) }} as PURL_FK_CAMT_ID,
	{{ json_extract_scalar('_airbyte_data', ['purl_fk_publisher'], ['purl_fk_publisher']) }} as PURL_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['purl_ftd_url'], ['purl_ftd_url']) }} as PURL_FTD_URL,
	{{ json_extract_scalar('_airbyte_data', ['purl_id'], ['purl_id']) }} as PURL_ID,
	{{ json_extract_scalar('_airbyte_data', ['purl_signup_url'], ['purl_signup_url']) }} as PURL_SIGNUP_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_3RD_PARTY_URLS') }} as table_alias
-- POSTBACK_3RD_PARTY_URLS
where 1 = 1
