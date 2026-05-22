{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFER_SOURCES_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'ALIAS',
        'CLICK_EXPIRATION',
        'CLICKID',
        'CREATED_AT',
        'CURRENCY',
        'ENABLE_IP_WHITELIST',
        'EVENT_TRACKING',
        'HINTS',
        'ID',
        'IP_WHITELIST',
        'NOTES',
        'OFFER_COUNT',
        'OFFER_URL',
        'POSTBACK_MODE',
        'POSTBACK_PROTECTED',
        'POSTBACK_STATUS',
        'POSTBACK_TOKEN',
        'POSTBACK_URL',
        'PRESET_ID',
        'SERIAL_NUMBER',
        'STAT',
        'STATUS',
        'SUB1',
        'SUB10',
        'SUB11',
        'SUB12',
        'SUB13',
        'SUB14',
        'SUB15',
        'SUB16',
        'SUB17',
        'SUB18',
        'SUB19',
        'SUB2',
        'SUB20',
        'SUB3',
        'SUB4',
        'SUB5',
        'SUB6',
        'SUB7',
        'SUB8',
        'SUB9',
        'SUBS',
        'SUM',
        'TITLE',
        'UPDATED_AT',
        'USER_ID',
    ]) }} as _AIRBYTE_OFFER_SOURCES_HASHID,
    tmp.*
from {{ ref('OFFER_SOURCES_AB2') }} tmp
-- OFFER_SOURCES
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

