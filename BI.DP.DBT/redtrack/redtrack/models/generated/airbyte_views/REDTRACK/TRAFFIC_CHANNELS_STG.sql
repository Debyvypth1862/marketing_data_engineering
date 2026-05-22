{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('TRAFFIC_CHANNELS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'ALIAS',
        'CAMPAIGN_COUNT',
        'COST_ID',
        'COST_LEVEL',
        'COST_MODELS',
        'CREATED_AT',
        'CURRENCY',
        'ENABLE_DIRECT_TRAFFIC',
        'ENABLE_IMPRESSIONS',
        'ENABLE_PARALLEL_TRACKING',
        'EXTERNAL_ID',
        'EXTERNAL_ID_ALIAS',
        'FORMATS',
        'GOOGLE_ANALYTICS_KEY',
        'ID',
        'IMP_COST_ID',
        'IMP_ID',
        'INTEGRATION_ID',
        'INTEGRATION_TYPES',
        'INTEGRATIONS',
        'POSTBACK_PIXEL',
        'POSTBACK_URL',
        'PRESET_ID',
        'REF_ID',
        'REF_ID_ALIAS',
        'SERIAL_NUMBER',
        'STAT',
        'STATUS',
        'SUBS',
        'TITLE',
        'TYPE',
        'UPDATED_AT',
        'USER_ID',
    ]) }} as _AIRBYTE_TRAFFIC_CHANNELS_HASHID,
    tmp.*
from {{ ref('TRAFFIC_CHANNELS_AB2') }} tmp
-- TRAFFIC_CHANNELS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

