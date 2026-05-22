{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_OPERATORS') }}
select
    {{ json_extract_scalar('_airbyte_data', ['contact_skype'], ['contact_skype']) }} as CONTACT_SKYPE,
    {{ json_extract_scalar('_airbyte_data', ['notes'], ['notes']) }} as NOTES,
    {{ json_extract_scalar('_airbyte_data', ['contact_phone'], ['contact_phone']) }} as CONTACT_PHONE,
    {{ json_extract_scalar('_airbyte_data', ['city'], ['city']) }} as CITY,
    {{ json_extract_scalar('_airbyte_data', ['api_url'], ['api_url']) }} as API_URL,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['language_id'], ['language_id']) }} as LANGUAGE_ID,
    {{ json_extract_scalar('_airbyte_data', ['contact_id'], ['contact_id']) }} as CONTACT_ID,
    {{ json_extract_scalar('_airbyte_data', ['contact_telegram'], ['contact_telegram']) }} as CONTACT_TELEGRAM,
    {{ json_extract_scalar('_airbyte_data', ['contact_email'], ['contact_email']) }} as CONTACT_EMAIL,
    {{ json_extract_scalar('_airbyte_data', ['login_url'], ['login_url']) }} as LOGIN_URL,
    {{ json_extract_scalar('_airbyte_data', ['url_logo'], ['url_logo']) }} as URL_LOGO,
    {{ json_extract_scalar('_airbyte_data', ['street_two'], ['street_two']) }} as STREET_TWO,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['manager_id'], ['manager_id']) }} as MANAGER_ID,
    {{ json_extract_scalar('_airbyte_data', ['street'], ['street']) }} as STREET,
    {{ json_extract_scalar('_airbyte_data', ['country_name'], ['country_name']) }} as COUNTRY_NAME,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['slug'], ['slug']) }} as SLUG,
    {{ json_extract_scalar('_airbyte_data', ['payment_method'], ['payment_method']) }} as PAYMENT_METHOD,
    {{ json_extract_scalar('_airbyte_data', ['contact_name'], ['contact_name']) }} as CONTACT_NAME,
    {{ json_extract_scalar('_airbyte_data', ['operator_platform_id'], ['operator_platform_id']) }} as OPERATOR_PLATFORM_ID,
    {{ json_extract_scalar('_airbyte_data', ['created_by'], ['created_by']) }} as CREATED_BY,
    {{ json_extract_scalar('_airbyte_data', ['api_ip_address'], ['api_ip_address']) }} as API_IP_ADDRESS,
    {{ json_extract_scalar('_airbyte_data', ['license'], ['license']) }} as LICENSE,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
    {{ json_extract_scalar('_airbyte_data', ['updated_by'], ['updated_by']) }} as UPDATED_BY,
    {{ json_extract_scalar('_airbyte_data', ['contact_wechat'], ['contact_wechat']) }} as CONTACT_WECHAT,
    {{ json_extract_scalar('_airbyte_data', ['region'], ['region']) }} as REGION,
    {{ json_extract_scalar('_airbyte_data', ['currency_id'], ['currency_id']) }} as CURRENCY_ID,
    {{ json_extract_scalar('_airbyte_data', ['admin_fee'], ['admin_fee']) }} as ADMIN_FEE,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_OPERATORS') }} as table_alias
-- OPERATORS
where 1 = 1

