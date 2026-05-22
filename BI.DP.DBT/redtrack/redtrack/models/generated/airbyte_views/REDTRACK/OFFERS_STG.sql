{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFERS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'ACTION_SOURCE_FB',
        'CAP',
        'CAP_ALERT',
        'CLCAP',
        'CLCAP_ALERT',
        'CLICK_CAP',
        'CLICK_CAP_PERIOD',
        'CLICK_CAP_TYPE',
        'COUNTRY_CODES',
        'CREATED_AT',
        'DEFAULT_CONVERSION_STATUS',
        'EVENT_SOURCE_URL_FB',
        'EXPIRES_AT',
        'FACEBOOK_PIXELS',
        'FINGERPRINT_SETTINGS',
        'ID',
        'NETWORK_TITLE',
        'NOTES',
        'PAYMENT',
        'POSTBACK_URL',
        'PROGRAM_ID',
        'SERIAL_NUMBER',
        'SNAPCHAT_MATCHING',
        'SNAPCHAT_PIXELS',
        'STAT',
        'STATUS',
        'TAGS',
        'TITLE',
        'UPDATED_AT',
        'URL',
        'USER_ID',
    ]) }} as _AIRBYTE_OFFERS_HASHID,
    tmp.*
from {{ ref('OFFERS_AB2') }} tmp
-- OFFERS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

