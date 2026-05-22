{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
	{{ json_extract_scalar('_airbyte_data', ['post_3rd_party_clickid'], ['post_3rd_party_clickid']) }} as POST_3RD_PARTY_CLICKID,
	{{ json_extract_scalar('_airbyte_data', ['post_click_httpcode'], ['post_click_httpcode']) }} as POST_CLICK_HTTPCODE,
	{{ json_extract_scalar('_airbyte_data', ['post_click_timestamp'], ['post_click_timestamp']) }} as POST_CLICK_TIMESTAMP,
	{{ json_extract_scalar('_airbyte_data', ['post_click_url'], ['post_click_url']) }} as POST_CLICK_URL,
	{{ json_extract_scalar('_airbyte_data', ['post_clickid'], ['post_clickid']) }} as POST_CLICKID,
	{{ json_extract_scalar('_airbyte_data', ['post_fk_camt_id'], ['post_fk_camt_id']) }} as POST_FK_CAMT_ID,
	{{ json_extract_scalar('_airbyte_data', ['post_fk_tracker'], ['post_fk_tracker']) }} as POST_FK_TRACKER,
	{{ json_extract_scalar('_airbyte_data', ['post_id'], ['post_id']) }} as POST_ID,
	{{ json_extract_scalar('_airbyte_data', ['post_subid'], ['post_subid']) }} as POST_SUBID,
	{{ json_extract_scalar('_airbyte_data', ['post_subid2'], ['post_subid2']) }} as POST_SUBID_2,
	{{ json_extract_scalar('_airbyte_data', ['post_subid3'], ['post_subid3']) }} as POST_SUBID_3,
	{{ json_extract_scalar('_airbyte_data', ['post_subid4'], ['post_subid4']) }} as POST_SUBID_4,
	{{ json_extract_scalar('_airbyte_data', ['post_subid5'], ['post_subid5']) }} as POST_SUBID_5,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	S3_PATH,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_3RD_PARTY_CLICK_LOG') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
{{ emitted_at_clause('_AIRBYTE_EMITTED_AT', source('BRC', '_AIRBYTE_RAW_CAMPAIGN_MATERIALS') ) }}
