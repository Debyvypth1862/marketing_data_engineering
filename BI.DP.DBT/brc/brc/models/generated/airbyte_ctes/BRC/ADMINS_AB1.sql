{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
	{{ json_extract_scalar('_airbyte_data', ['admi_color'], ['admi_color']) }} as ADMI_COLOR,
	{{ json_extract_scalar('_airbyte_data', ['admi_created'], ['admi_created']) }} as ADMI_CREATED,
	{{ json_extract_scalar('_airbyte_data', ['admi_deleted'], ['admi_deleted']) }} as ADMI_DELETED,
	{{ json_extract_scalar('_airbyte_data', ['admi_display_name'], ['admi_display_name']) }} as ADMI_DISPLAY_NAME,
	{{ json_extract_scalar('_airbyte_data', ['admi_email'], ['admi_email']) }} as ADMI_EMAIL,
	{{ json_extract_scalar('_airbyte_data', ['admi_id'], ['admi_id']) }} as ADMI_ID,
	{{ json_extract_scalar('_airbyte_data', ['admi_ip'], ['admi_ip']) }} as ADMI_IP,
	{{ json_extract_scalar('_airbyte_data', ['admi_last_login'], ['admi_last_login']) }} as ADMI_LAST_LOGIN,
	{{ json_extract_scalar('_airbyte_data', ['admi_level'], ['admi_level']) }} as ADMI_LEVEL,
	{{ json_extract_scalar('_airbyte_data', ['admi_password'], ['admi_password']) }} as ADMI_PASSWORD,
	{{ json_extract_scalar('_airbyte_data', ['admi_publisher_manager'], ['admi_publisher_manager']) }} as ADMI_PUBLISHER_MANAGER,
	{{ json_extract_scalar('_airbyte_data', ['admi_skype'], ['admi_skype']) }} as ADMI_SKYPE,
	{{ json_extract_scalar('_airbyte_data', ['admi_telegram'], ['admi_telegram']) }} as ADMI_TELEGRAM,
	{{ json_extract_scalar('_airbyte_data', ['admi_username'], ['admi_username']) }} as ADMI_USERNAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_ADMINS') }} as table_alias

WHERE 1=1
