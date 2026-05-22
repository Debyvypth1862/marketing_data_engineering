{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "SOFTSWISS",
    tags = ["top-level-intermediate"]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('ACTIVITY_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key(
        ['TRACKER_LOGIN_ID',
        'START_DATE',
        'END_DATE',
        'DATE',
        'BRAND_ID',
        'CAMPAIGN_ID',
        'DYNAMIC_TAG_CLICKID',
        'VISITS_COUNT',
        'REGISTRATIONS_COUNT',
        'CURRENCY',
        'NGR',
        'DEPOSITS_SUM',
        'DEPOSIT_COUNT',
        'FIRST_DEPOSITS_COUNT',
        'FIRST_DEPOSITS_SUM',
    ]) }} as _AIRBYTE_ACTIVITY_REPORT_HASHID,
    tmp.*
from {{ ref('ACTIVITY_REPORT_AB2')}} tmp 
-- ACTIVITY_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
