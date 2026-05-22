{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['user_address'], ['user_address']) }} as USER_ADDRESS,
        {{ json_extract_scalar('_airbyte_data', ['user_city'], ['user_city']) }} as USER_CITY,
        {{ json_extract_scalar('_airbyte_data', ['user_country'], ['user_country']) }} as USER_COUNTRY,
        {{ json_extract_scalar('_airbyte_data', ['user_created'], ['user_created']) }} as USER_CREATED,
        {{ json_extract_scalar('_airbyte_data', ['user_display_name'], ['user_display_name']) }} as USER_DISPLAY_NAME,
        {{ json_extract_scalar('_airbyte_data', ['user_email'], ['user_email']) }} as USER_EMAIL,
        {{ json_extract_scalar('_airbyte_data', ['user_firstname'], ['user_firstname']) }} as USER_FIRSTNAME,
        {{ json_extract_scalar('_airbyte_data', ['user_id'], ['user_id']) }} as USER_ID,
        {{ json_extract_scalar('_airbyte_data', ['user_ip'], ['user_ip']) }} as USER_IP,
        {{ json_extract_scalar('_airbyte_data', ['user_lastname'], ['user_lastname']) }} as USER_LASTNAME,
        {{ json_extract_scalar('_airbyte_data', ['user_password'], ['user_password']) }} as USER_PASSWORD,
        {{ json_extract_scalar('_airbyte_data', ['user_phone'], ['user_phone']) }} as USER_PHONE,
        {{ json_extract_scalar('_airbyte_data', ['user_ref'], ['user_ref']) }} as USER_REF,
        {{ json_extract_scalar('_airbyte_data', ['user_skype'], ['user_skype']) }} as USER_SKYPE,
        {{ json_extract_scalar('_airbyte_data', ['user_status'], ['user_status']) }} as USER_STATUS,
        {{ json_extract_scalar('_airbyte_data', ['user_username'], ['user_username']) }} as USER_USERNAME,
        {{ json_extract_scalar('_airbyte_data', ['user_zipcode'], ['user_zipcode']) }} as USER_ZIPCODE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_USERS') }} as table_alias
-- USERS
where 1 = 1
