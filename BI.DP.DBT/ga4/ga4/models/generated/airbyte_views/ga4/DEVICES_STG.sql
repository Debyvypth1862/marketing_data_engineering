{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('DEVICES_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'PROPERTY_ID',
        'UUID',
        'DATE',
        'DEVICE_CATEGORY',
        'OPERATING_SYSTEM',
        'BROWSER',
        'TOTAL_USERS',
        'NEW_USERS',
        'SESSIONS',
        'SESSIONS_PERUSER',
        'AVERAGE_SESSION_DURATION',
        'SCREEN_PAGE_VIEWS',
        'SCREEN_PAGE_VIEWS_PER_SESSION',
        'BOUNCE_RATE'
    ]) }} as _AIRBYTE_DEVICES_HASHID,
    tmp.*
from {{ ref('DEVICES_AB2') }} tmp

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

