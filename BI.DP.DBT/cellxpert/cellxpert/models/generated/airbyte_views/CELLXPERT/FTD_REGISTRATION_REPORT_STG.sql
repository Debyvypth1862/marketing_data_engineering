{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "CELLXPERT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('FTD_REGISTRATION_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'DATE',
        'AFP',
        'COMMISSION',
        'COMMISSIONS',
        'COUNTRY',
        'DEPOSITS',
        'DEPOSIT_COUNT',
        'EXTERNAL_DATE',
        'FIRST_DEPOSIT',
        'FIRST_DEPOSIT_DATE',
        'GENERIC_1',
        'GENERIC_2',
        'NET_DEPOSITS',
        'PL',
        'QUALIFICATION_DATE',
        'REGISTRATION_DATE',
        'STATUS',
        'TRACKING_CODE',
        'USERID',
        'WITHDRAWALS',
        'TRACKER_LOGIN_ID',
        
    ]) }} as _AIRBYTE_FTD_REGISTRATION_REPORT_HASHID,
    tmp.*
from {{ ref('FTD_REGISTRATION_REPORT_AB2') }} tmp
-- FTD_REGISTRATION_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

