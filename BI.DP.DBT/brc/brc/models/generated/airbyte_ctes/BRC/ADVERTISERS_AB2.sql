{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(ADVE_AFFILIATE_LOGIN_URL as {{ dbt_utils.type_string() }}) as ADVE_AFFILIATE_LOGIN_URL,
	try_cast(ADVE_AFFILIATE_SYSTEM as {{ dbt_utils.type_string() }}) as ADVE_AFFILIATE_SYSTEM,
	try_cast(ADVE_AFFILIATE_SYSTEM_2ND as {{ dbt_utils.type_string() }}) as ADVE_AFFILIATE_SYSTEM_2ND,
	try_cast(ADVE_CONTACT_EMAIL as {{ dbt_utils.type_string() }}) as ADVE_CONTACT_EMAIL,
	try_cast(ADVE_CONTACT_NAME as {{ dbt_utils.type_string() }}) as ADVE_CONTACT_NAME,
	try_cast(ADVE_CONTACT_NOTE as {{ dbt_utils.type_string() }}) as ADVE_CONTACT_NOTE,
	try_cast(ADVE_CONTACT_SKYPE as {{ dbt_utils.type_string() }}) as ADVE_CONTACT_SKYPE,
	try_cast(ADVE_COUNTRIES as {{ dbt_utils.type_string() }}) as ADVE_COUNTRIES,
	try_cast(ADVE_DEAL as {{ dbt_utils.type_string() }}) as ADVE_DEAL,
	try_cast(ADVE_DELETED as {{ dbt_utils.type_float() }}) as ADVE_DELETED,
	try_cast(ADVE_DESCRIPTION as {{ dbt_utils.type_string() }}) as ADVE_DESCRIPTION,
	try_cast(ADVE_ID as {{ dbt_utils.type_float() }}) as ADVE_ID,
	try_cast(ADVE_IMPORT_ACTIVE as {{ dbt_utils.type_float() }}) as ADVE_IMPORT_ACTIVE,
	try_cast(ADVE_META_INFO as {{ dbt_utils.type_string() }}) as ADVE_META_INFO,
	try_cast(ADVE_NAME as {{ dbt_utils.type_string() }}) as ADVE_NAME,
	try_cast(ADVE_NEGATIVE_CARRYOVER as {{ dbt_utils.type_float() }}) as ADVE_NEGATIVE_CARRYOVER,
	try_cast(ADVE_NEGATIVE_CARRYOVER_REV_TO_CPA as {{ dbt_utils.type_float() }}) as ADVE_NEGATIVE_CARRYOVER_REV_TO_CPA,
	try_cast(ADVE_SCREENSHOT as {{ dbt_utils.type_string() }}) as ADVE_SCREENSHOT,
	try_cast(ADVE_SUBID_ACTIVATED as {{ dbt_utils.type_float() }}) as ADVE_SUBID_ACTIVATED,
	try_cast(ADVE_SUBID_URL as {{ dbt_utils.type_string() }}) as ADVE_SUBID_URL,
	try_cast(ADVE_TRACKERLOGIN_HIDDEN as {{ dbt_utils.type_float() }}) as ADVE_TRACKERLOGIN_HIDDEN,
	try_cast(ADVE_TRACKING_DOMAIN as {{ dbt_utils.type_string() }}) as ADVE_TRACKING_DOMAIN,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISERS_AB1') }}
-- ADVERTISERS
where 1 = 1