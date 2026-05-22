{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_AFFILIATES') }}
select
    {{ json_extract_scalar('_airbyte_data', ['whatsapp'], ['whatsapp']) }} as WHATSAPP,
    {{ json_extract_scalar('_airbyte_data', ['country'], ['country']) }} as COUNTRY,
    {{ json_extract_scalar('_airbyte_data', ['notes'], ['notes']) }} as NOTES,
    {{ json_extract_scalar('_airbyte_data', ['city'], ['city']) }} as CITY,
    {{ json_extract_scalar('_airbyte_data', ['timezone'], ['timezone']) }} as TIMEZONE,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['language_id'], ['language_id']) }} as LANGUAGE_ID,
    {{ json_extract_scalar('_airbyte_data', ['contact_id'], ['contact_id']) }} as CONTACT_ID,
    {{ json_extract_scalar('_airbyte_data', ['zip_code'], ['zip_code']) }} as ZIP_CODE,
    {{ json_extract_scalar('_airbyte_data', ['fraud_score'], ['fraud_score']) }} as FRAUD_SCORE,
    {{ json_extract_scalar('_airbyte_data', ['skype'], ['skype']) }} as SKYPE,
    {{ json_extract_scalar('_airbyte_data', ['url_logo'], ['url_logo']) }} as URL_LOGO,
    {{ json_extract_scalar('_airbyte_data', ['street_two'], ['street_two']) }} as STREET_TWO,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['manager_id'], ['manager_id']) }} as MANAGER_ID,
    {{ json_extract_scalar('_airbyte_data', ['street'], ['street']) }} as STREET,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['signal'], ['signal']) }} as SIGNAL,
    {{ json_extract_scalar('_airbyte_data', ['slug'], ['slug']) }} as SLUG,
    {{ json_extract_scalar('_airbyte_data', ['email'], ['email']) }} as EMAIL,
    {{ json_extract_scalar('_airbyte_data', ['telegram'], ['telegram']) }} as TELEGRAM,
    {{ json_extract_scalar('_airbyte_data', ['url'], ['url']) }} as URL,
    {{ json_extract_scalar('_airbyte_data', ['discord'], ['discord']) }} as DISCORD,
    {{ json_extract_scalar('_airbyte_data', ['phone'], ['phone']) }} as PHONE,
    {{ json_extract_scalar('_airbyte_data', ['name'], ['name']) }} as NAME,
    {{ json_extract_scalar('_airbyte_data', ['region'], ['region']) }} as REGION,
    {{ json_extract_scalar('_airbyte_data', ['currency_id'], ['currency_id']) }} as CURRENCY_ID,
    {{ json_extract_scalar('_airbyte_data', ['status'], ['status']) }} as STATUS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_AFFILIATES') }} as table_alias
-- AFFILIATES
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

