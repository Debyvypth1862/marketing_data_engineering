{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OPERATORS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'CONTACT_SKYPE',
        'NOTES',
        'CONTACT_PHONE',
        'CITY',
        'API_URL',
        'CREATED_AT',
        'LANGUAGE_ID',
        'CONTACT_ID',
        'CONTACT_TELEGRAM',
        'CONTACT_EMAIL',
        'LOGIN_URL',
        'URL_LOGO',
        'STREET_TWO',
        'UPDATED_AT',
        'MANAGER_ID',
        'STREET',
        'COUNTRY_NAME',
        'ID',
        'SLUG',
        'PAYMENT_METHOD',
        'CONTACT_NAME',
        'OPERATOR_PLATFORM_ID',
        'CREATED_BY',
        'API_IP_ADDRESS',
        'LICENSE',
        'NAME',
        'UPDATED_BY',
        'CONTACT_WECHAT',
        'REGION',
        'CURRENCY_ID',
        'STATUS',
    ]) }} as _AIRBYTE_OPERATORS_HASHID,
    tmp.*
from {{ ref('OPERATORS_AB2') }} tmp
-- OPERATORS
where 1 = 1

