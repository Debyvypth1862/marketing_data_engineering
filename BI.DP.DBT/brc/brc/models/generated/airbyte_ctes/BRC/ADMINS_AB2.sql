{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(ADMI_COLOR as {{ dbt_utils.type_string() }}) as ADMI_COLOR,
	try_cast(ADMI_CREATED as {{ dbt_utils.type_string() }}) as ADMI_CREATED,
	try_cast(ADMI_DELETED as {{ dbt_utils.type_float() }}) as ADMI_DELETED,
	try_cast(ADMI_DISPLAY_NAME as {{ dbt_utils.type_string() }}) as ADMI_DISPLAY_NAME,
	try_cast(ADMI_EMAIL as {{ dbt_utils.type_string() }}) as ADMI_EMAIL,
	try_cast(ADMI_ID as {{ dbt_utils.type_float() }}) as ADMI_ID,
	try_cast(ADMI_IP as {{ dbt_utils.type_string() }}) as ADMI_IP,
	try_cast(ADMI_LAST_LOGIN as {{ dbt_utils.type_string() }}) as ADMI_LAST_LOGIN,
	try_cast(ADMI_LEVEL as {{ dbt_utils.type_float() }}) as ADMI_LEVEL,
	try_cast(ADMI_PASSWORD as {{ dbt_utils.type_string() }}) as ADMI_PASSWORD,
	try_cast(ADMI_PUBLISHER_MANAGER as {{ dbt_utils.type_float() }}) as ADMI_PUBLISHER_MANAGER,
	try_cast(ADMI_SKYPE as {{ dbt_utils.type_string() }}) as ADMI_SKYPE,
	try_cast(ADMI_TELEGRAM as {{ dbt_utils.type_string() }}) as ADMI_TELEGRAM,
	try_cast(ADMI_USERNAME as {{ dbt_utils.type_string() }}) as ADMI_USERNAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADMINS_AB1') }}
where 1 = 1