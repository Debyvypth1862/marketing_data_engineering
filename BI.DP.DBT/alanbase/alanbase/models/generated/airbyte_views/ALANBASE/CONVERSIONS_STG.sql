{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "ALANBASE",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('CONVERSIONS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'CONVERSION_ID',
        'STATUS',
        'CONVERSION_DATETIME',
        'PAYMENT_MODEL',
        'PAYOUT',
        'PAYOUT_CURRENCY',
        'SUB1',
        'SUB2',
        'EDITED_BY_MANAGER',
        'CLICK_ID',
        'CLICK_DATETIME',
        'CLICK_REDIRECT_URL',
        'CLICK_IP',
        'BROWSER',
        'OS',
        'DEVICE_TYPE',
        'COUNTRY',
        'REFERER',
        'CONDITION_ID',
        'IS_QUALIFICATION',
        'USER_AGENT',
        'LANDING_ID',
        'GOAL',
        'OFFER_ID',
        'OFFER_NAME',
        'OFFER_TAGS',
        'PARTNER_ID',
        'PARTNER_EMAIL',
        'DATE',
        'TRACKER_LOGIN_ID',
        'DECLINE_REASON',
    ]) }} as _AIRBYTE_CONVERSIONS_HASHID,
    tmp.*
from {{ ref('CONVERSIONS_AB2') }} tmp
-- CONVERSIONS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

