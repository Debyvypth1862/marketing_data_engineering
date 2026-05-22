{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "SWEEP",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('DYNAMIC_VARIABLES_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'DATE',
        'DEPOSITS',
        'WITHDRAWALS',
        'AFP',
        'NET_DEPOSITS',
        'USERID',
        'COMMISSIONS',
        'BRAND',
        'VOLUME',
        'DEPOSIT_COUNT',
        'COMMISSION_COUNT',
        'POSITION_COUNT',
        'PL',
        'TRACKING_CODE',
        'TRACKER_LOGIN_ID',
    ]) }} as _AIRBYTE_DYNAMIC_VARIABLES_REPORT_HASHID,
    tmp.*
from {{ ref('DYNAMIC_VARIABLES_REPORT_AB2') }} tmp
-- DYNAMIC_VARIABLES_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

