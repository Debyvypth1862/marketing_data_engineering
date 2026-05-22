{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['adve_affiliate_login_url'], ['adve_affiliate_login_url']) }} as ADVE_AFFILIATE_LOGIN_URL,
	{{ json_extract_scalar('_airbyte_data', ['adve_affiliate_system'], ['adve_affiliate_system']) }} as ADVE_AFFILIATE_SYSTEM,
	{{ json_extract_scalar('_airbyte_data', ['adve_affiliate_system_2nd'], ['adve_affiliate_system_2nd']) }} as ADVE_AFFILIATE_SYSTEM_2ND,
	{{ json_extract_scalar('_airbyte_data', ['adve_contact_email'], ['adve_contact_email']) }} as ADVE_CONTACT_EMAIL,
	{{ json_extract_scalar('_airbyte_data', ['adve_contact_name'], ['adve_contact_name']) }} as ADVE_CONTACT_NAME,
	{{ json_extract_scalar('_airbyte_data', ['adve_contact_note'], ['adve_contact_note']) }} as ADVE_CONTACT_NOTE,
	{{ json_extract_scalar('_airbyte_data', ['adve_contact_skype'], ['adve_contact_skype']) }} as ADVE_CONTACT_SKYPE,
	{{ json_extract_scalar('_airbyte_data', ['adve_countries'], ['adve_countries']) }} as ADVE_COUNTRIES,
	{{ json_extract_scalar('_airbyte_data', ['adve_deal'], ['adve_deal']) }} as ADVE_DEAL,
	{{ json_extract_scalar('_airbyte_data', ['adve_deleted'], ['adve_deleted']) }} as ADVE_DELETED,
	{{ json_extract_scalar('_airbyte_data', ['adve_description'], ['adve_description']) }} as ADVE_DESCRIPTION,
	{{ json_extract_scalar('_airbyte_data', ['adve_id'], ['adve_id']) }} as ADVE_ID,
	{{ json_extract_scalar('_airbyte_data', ['adve_import_active'], ['adve_import_active']) }} as ADVE_IMPORT_ACTIVE,
	{{ json_extract_scalar('_airbyte_data', ['adve_meta_info'], ['adve_meta_info']) }} as ADVE_META_INFO,
	{{ json_extract_scalar('_airbyte_data', ['adve_name'], ['adve_name']) }} as ADVE_NAME,
	{{ json_extract_scalar('_airbyte_data', ['adve_negative_carryover'], ['adve_negative_carryover']) }} as ADVE_NEGATIVE_CARRYOVER,
	{{ json_extract_scalar('_airbyte_data', ['adve_negative_carryover_rev_to_cpa'], ['adve_negative_carryover_rev_to_cpa']) }} as ADVE_NEGATIVE_CARRYOVER_REV_TO_CPA,
	{{ json_extract_scalar('_airbyte_data', ['adve_screenshot'], ['adve_screenshot']) }} as ADVE_SCREENSHOT,
	{{ json_extract_scalar('_airbyte_data', ['adve_subid_activated'], ['adve_subid_activated']) }} as ADVE_SUBID_ACTIVATED,
	{{ json_extract_scalar('_airbyte_data', ['adve_subid_url'], ['adve_subid_url']) }} as ADVE_SUBID_URL,
	{{ json_extract_scalar('_airbyte_data', ['adve_trackerlogin_hidden'], ['adve_trackerlogin_hidden']) }} as ADVE_TRACKERLOGIN_HIDDEN,
	{{ json_extract_scalar('_airbyte_data', ['adve_tracking_domain'], ['adve_tracking_domain']) }} as ADVE_TRACKING_DOMAIN,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISERS') }} as table_alias
-- ADVERTISERS
where 1 = 1
