{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('AFFILIATES_STG') }}
select
    
    WHATSAPP,
    COUNTRY,
    NOTES,
    CITY,
    TIMEZONE,
    CREATED_AT,
    LANGUAGE_ID,
    CONTACT_ID,
    ZIP_CODE,
    FRAUD_SCORE,
    SKYPE,
    URL_LOGO,
    STREET_TWO,
    UPDATED_AT,
    MANAGER_ID,
    STREET,
    ID,
    SIGNAL,
    SLUG,
    EMAIL,
    TELEGRAM,
    URL,
    DISCORD,
    PHONE,
    NAME,
    REGION,
    CURRENCY_ID,
    STATUS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_AFFILIATES_HASHID
from {{ ref('AFFILIATES_STG') }}
-- AFFILIATES from {{ source('BRT', '_AIRBYTE_RAW_AFFILIATES') }}
where 1 = 1


