{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['post_3rd_party_clickid'], ['post_3rd_party_clickid']) }} as POST_3RD_PARTY_CLICKID,
	{{ json_extract_scalar('_airbyte_data', ['post_clickid'], ['post_clickid']) }} as POST_CLICKID,
	{{ json_extract_scalar('_airbyte_data', ['post_cpa_postback_attempts'], ['post_cpa_postback_attempts']) }} as POST_CPA_POSTBACK_ATTEMPTS,
	{{ json_extract_scalar('_airbyte_data', ['post_cpa_send_httpcode'], ['post_cpa_send_httpcode']) }} as POST_CPA_SEND_HTTPCODE,
	{{ json_extract_scalar('_airbyte_data', ['post_cpa_send_url'], ['post_cpa_send_url']) }} as POST_CPA_SEND_URL,
	{{ json_extract_scalar('_airbyte_data', ['post_cpa_timestamp'], ['post_cpa_timestamp']) }} as POST_CPA_TIMESTAMP,
	{{ json_extract_scalar('_airbyte_data', ['post_fk_camt_id'], ['post_fk_camt_id']) }} as POST_FK_CAMT_ID,
	{{ json_extract_scalar('_airbyte_data', ['post_fk_tracker'], ['post_fk_tracker']) }} as POST_FK_TRACKER,
	{{ json_extract_scalar('_airbyte_data', ['post_ftd_postback_attempts'], ['post_ftd_postback_attempts']) }} as POST_FTD_POSTBACK_ATTEMPTS,
	{{ json_extract_scalar('_airbyte_data', ['post_ftd_send_httpcode'], ['post_ftd_send_httpcode']) }} as POST_FTD_SEND_HTTPCODE,
	{{ json_extract_scalar('_airbyte_data', ['post_ftd_send_url'], ['post_ftd_send_url']) }} as POST_FTD_SEND_URL,
	{{ json_extract_scalar('_airbyte_data', ['post_ftd_timestamp'], ['post_ftd_timestamp']) }} as POST_FTD_TIMESTAMP,
	{{ json_extract_scalar('_airbyte_data', ['post_id'], ['post_id']) }} as POST_ID,
	{{ json_extract_scalar('_airbyte_data', ['post_signup_postback_attempts'], ['post_signup_postback_attempts']) }} as POST_SIGNUP_POSTBACK_ATTEMPTS,
	{{ json_extract_scalar('_airbyte_data', ['post_signup_send_httpcode'], ['post_signup_send_httpcode']) }} as POST_SIGNUP_SEND_HTTPCODE,
	{{ json_extract_scalar('_airbyte_data', ['post_signup_send_url'], ['post_signup_send_url']) }} as POST_SIGNUP_SEND_URL,
	{{ json_extract_scalar('_airbyte_data', ['post_signup_timestamp'], ['post_signup_timestamp']) }} as POST_SIGNUP_TIMESTAMP,
	{{ json_extract_scalar('_airbyte_data', ['post_subid'], ['post_subid']) }} as POST_SUBID,
	{{ json_extract_scalar('_airbyte_data', ['post_subid2'], ['post_subid2']) }} as POST_SUBID_2,
	{{ json_extract_scalar('_airbyte_data', ['post_subid3'], ['post_subid3']) }} as POST_SUBID_3,
	{{ json_extract_scalar('_airbyte_data', ['post_subid4'], ['post_subid4']) }} as POST_SUBID_4,
	{{ json_extract_scalar('_airbyte_data', ['post_subid5'], ['post_subid5']) }} as POST_SUBID_5,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_3RD_PARTY_LOG') }} as table_alias
-- POSTBACK_3RD_PARTY_LOG
where 1 = 1
