{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['cdre_accepted_seen'], ['cdre_accepted_seen']) }} as CDRE_ACCEPTED_SEEN,
	{{ json_extract_scalar('_airbyte_data', ['cdre_fk_camp_deal'], ['cdre_fk_camp_deal']) }} as CDRE_FK_CAMP_DEAL,
	{{ json_extract_scalar('_airbyte_data', ['cdre_fk_publisher'], ['cdre_fk_publisher']) }} as CDRE_FK_PUBLISHER,
	{{ json_extract_scalar('_airbyte_data', ['cdre_id'], ['cdre_id']) }} as CDRE_ID,
	{{ json_extract_scalar('_airbyte_data', ['cdre_note'], ['cdre_note']) }} as CDRE_NOTE,
	{{ json_extract_scalar('_airbyte_data', ['cdre_request_date'], ['cdre_request_date']) }} as CDRE_REQUEST_DATE,
	{{ json_extract_scalar('_airbyte_data', ['cdre_status'], ['cdre_status']) }} as CDRE_STATUS,
	{{ json_extract_scalar('_airbyte_data', ['cdre_update_date'], ['cdre_update_date']) }} as CDRE_UPDATE_DATE,
	{{ json_extract_scalar('_airbyte_data', ['cdre_updated_by'], ['cdre_updated_by']) }} as CDRE_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_DEAL_REQUESTS') }} as table_alias
-- CAMPAIGN_DEAL_REQUESTS
where 1 = 1
