{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('DAILY_ACTIVE_USERS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'ACTIVE1DAYUSERS',
        'PROPERTY_ID',
        'UUID',
        'DATE',
    
    ]) }} as _AIRBYTE_DAILY_ACTIVE_USERS_HASHID,
    tmp.*
from {{ ref('DAILY_ACTIVE_USERS_AB2') }} tmp

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

