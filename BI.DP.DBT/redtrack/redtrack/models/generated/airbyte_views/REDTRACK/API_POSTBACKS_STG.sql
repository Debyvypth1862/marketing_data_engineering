{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "REDTRACK",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('API_POSTBACKS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'ALIAS',
        'ATTEMPTS',
        'CAMPAIGN',
        'CAMPAIGN_ID',
        'CONVERSION_ID',
        'CREATED_AT',
        'DESTINATION',
        'ERROR',
        'ID',
        'PREVIOUS_AT',
        'REF_ID',
        'SOURCE',
        'SOURCE_ID',
        'STATUS',
        'TOTAL',
        'TRACK_ID',
        'TYPE',
        'USER_ID'
    ]) }} as _AIRBYTE_API_POSTBACKS_HASHID,
    tmp.*
from {{ ref('API_POSTBACKS_AB2') }} tmp
-- API_POSTBACKS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

