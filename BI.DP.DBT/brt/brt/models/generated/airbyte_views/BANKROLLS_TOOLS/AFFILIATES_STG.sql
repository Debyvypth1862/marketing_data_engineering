{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('AFFILIATES_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'WHATSAPP',
        'COUNTRY',
        'NOTES',
        'CITY',
        'TIMEZONE',
        'CREATED_AT',
        'LANGUAGE_ID',
        'CONTACT_ID',
        'ZIP_CODE',
        'FRAUD_SCORE',
        'SKYPE',
        'URL_LOGO',
        'STREET_TWO',
        'UPDATED_AT',
        'MANAGER_ID',
        'STREET',
        'ID',
        'SIGNAL',
        'SLUG',
        'EMAIL',
        'TELEGRAM',
        'URL',
        'DISCORD',
        'PHONE',
        'NAME',
        'REGION',
        'CURRENCY_ID',
        'STATUS',
    ]) }} as _AIRBYTE_AFFILIATES_HASHID,
    tmp.*
from {{ ref('AFFILIATES_AB2') }} tmp
-- AFFILIATES
where 1 = 1

