{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LOCATIONS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'AVERAGE_SESSION_DURATION',
        'BOUNCE_RATE',
        'CITY',
        'COUNTRY',
        'DATE',
        'NEW_USERS',
        'PROPERTY_ID',
        'REGION',
        'SCREEN_PAGE_VIEWS',
        'SCREEN_PAGE_VIEWS_PER_SESSION',
        'SESSIONS',
        'SESSIONS_PERUSER',
        'TOTAL_USERS',
        'UUID'
    ]) }} as _AIRBYTE_LOCATIONS_HASHID,
    tmp.*
from {{ ref('LOCATIONS_AB2') }} tmp

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

