{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OPERATORS_STG') }}
select
    
    CONTACT_SKYPE,
    NOTES,
    CONTACT_PHONE,
    CITY,
    API_URL,
    CREATED_AT,
    LANGUAGE_ID,
    CONTACT_ID,
    CONTACT_TELEGRAM,
    CONTACT_EMAIL,
    LOGIN_URL,
    URL_LOGO,
    STREET_TWO,
    UPDATED_AT,
    MANAGER_ID,
    STREET,
    COUNTRY_NAME,
    ID,
    SLUG,
    PAYMENT_METHOD,
    CONTACT_NAME,
    OPERATOR_PLATFORM_ID,
    CREATED_BY,
    API_IP_ADDRESS,
    LICENSE,
    NAME,
    UPDATED_BY,
    CONTACT_WECHAT,
    REGION,
    CURRENCY_ID,
    STATUS,
    ADMIN_FEE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OPERATORS_HASHID
from {{ ref('OPERATORS_STG') }}
-- OPERATORS from {{ source('BRT', '_AIRBYTE_RAW_OPERATORS') }}
where 1 = 1


