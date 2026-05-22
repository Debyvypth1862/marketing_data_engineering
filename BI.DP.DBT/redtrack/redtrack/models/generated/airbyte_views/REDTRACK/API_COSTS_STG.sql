{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('API_COSTS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'CAMPAIGN',
        'CAMPAIGN_ID',
        'COUNTRY',
        'CREATED_AT',
        'CURRENCY',
        'ID',
        'LEVEL',
        'PERIOD',
        'RT_AD_ID',
        'RT_ADGROUP_ID',
        'RT_CAMPAIGN_ID',
        'RT_PLACEMENT_ID',
        'SOURCE_ALIAS',
        'SOURCE_COST',
        'SOURCE_TIMEZONE',
        'TIME_FROM',
        'TIME_TO',
        'USER_ID',
        'USER_TIMEZONE',
    ]) }} as _AIRBYTE_API_COSTS_HASHID,
    tmp.*
from {{ ref('API_COSTS_AB2') }} tmp
-- API_COSTS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

