{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "NETREFER",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('DYNAMIC_VARIABLES_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'DATE',
        'AFFILIATES_ID',
        'MARKETING_SOURCE_ID',
        'MARKETING_SOURCE_NAME',
        'MEDIA_ID',
        'ACTIVE_CUSTOMERS',
        'CLICKS',
        'DEPOSITS',
        'DEPOSITING_CUSTOMERS',
        'FIRST_TIME_ACTIVE_CUSTOMERS',
        'FIRST_TIME_DEPOSITING_CUSTOMER',
        'NET_REVENUE',
        'NEW_ACTIVE_CUSTOMERS',
        'NEW_DEPOSITING_CUSTOMERS',
        'SIGNUPS',
        'UNIQUE_CLICKS',
        'CLICK_ID',
        'TRACKER_LOGIN_ID',
    ]) }} as _AIRBYTE_DYNAMIC_VARIABLES_REPORT_HASHID,
    tmp.*
from {{ ref('DYNAMIC_VARIABLES_REPORT_AB2') }} tmp
-- DYNAMIC_VARIABLES_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

