{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('CAMPAIGNS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'CACHE_BUSTER_ENABLED',
        'COST_MODEL',
        'COUPON',
        'CPC',
        'CREATED_AT',
        'CREATIVES',
        'CUSTOM_CONV_TYPE_CONV_SUBS_GEO_PAYOUTS',
        'CUSTOM_CONV_TYPE_CONV_SUBS_PAYOUTS',
        'CUSTOM_CONVERSION_SUBS_PAYOUTS',
        'CUSTOM_CONVERSION_TYPE_PAYOUTS',
        'CUSTOM_GEO_OS_PAYOUTS',
        'CUSTOM_GEO_PAYOUTS',
        'CUSTOM_PUB_PAYOUTS',
        'CUSTOM_PUB_SUB_GEO_PAYOUTS',
        'CUSTOM_PUB_SUB_PAYOUTS',
        'CUSTOM_PUB_SUB_THROTTLES',
        'CUSTOM_PUB_THROTTLES',
        'CUSTOM_THROTTLES',
        'DOMAIN_ID',
        'EXPIRES_AT',
        'ID',
        'IMPRESSION_POSTBACKS',
        'IMPRESSION_URL',
        'INTEGRATION_POSTBACK',
        'INTEGRATIONS',
        'NOTES',
        'NOTIFICATIONS',
        'PIXELS',
        'POSTBACK_URL',
        'POSTBACKS',
        'PUBLISHER_DETAILS',
        'REDIRECT_TYPE',
        'REV_SHARE',
        'SERIAL_NUMBER',
        'SOURCE_CAMPAIGN_ID',
        'SOURCE_CAMPAIGNS',
        'SOURCE_ID',
        'SOURCE_TITLE',
        'STAT',
        'STATUS',
        'STREAMS',
        'TAGS',
        'TITLE',
        'TRACKBACK_URL',
        'TYPE',
        'UPDATED_AT',
        'USER_ID',
    ]) }} as _AIRBYTE_CAMPAIGNS_HASHID,
    tmp.*
from {{ ref('CAMPAIGNS_AB2') }} tmp
-- CAMPAIGNS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

