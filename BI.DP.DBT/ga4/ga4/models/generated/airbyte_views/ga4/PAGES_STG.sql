{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "GOOGLE_ANALYTICS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('PAGES_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'BOUNCE_RATE',
        'DATE',
        'HOST_NAME',
        'PAGE_PATH_PLUS_QUERY_STRING',
        'PROPERTY_ID',
        'SCREEN_PAGE_VIEWS',
        'UUID',
    ]) }} as _AIRBYTE_PAGES_HASHID,
    tmp.*
from {{ ref('PAGES_AB2') }} tmp

where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

