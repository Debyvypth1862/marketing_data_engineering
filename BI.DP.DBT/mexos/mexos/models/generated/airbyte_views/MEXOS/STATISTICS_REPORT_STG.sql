{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "MEXOS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('STATISTICS_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'DATE',
        'CASINO_NET_GAMING_COMMISSION',
        'CASINO_RFD_AMT',
        'CASINO_RFD_CNT',
        'CASINO_SIGNUPS_CNT',
        'COMMISSION',
        'DEPOSIT_CNT',
        'IMPRESSIONS',
        'NET_GAMING_AFTER_DEDUCTION',
        'SPORT_NET_GAMING_COMMISSION',
        'SPORT_RFD_AMT',
        'SPORT_RFD_CNT',
        'SPORT_SIGNUPS_CNT',
        'TOTAL_BONUSES_EUR',
        'UNIQUE_CLICKS',
        'CLICK_ID',
        'WINS',
        'WITHDRAWAL_AMT',
        'WITHDRAWAL_CNT',
        'TOTAL_DEPOSIT_CNT',
        'TOTAL_DEPOSIT_AMT',
        'TRACKER_LOGIN_ID',
    ]) }} as _AIRBYTE_STATISTICS_REPORT_HASHID,
    tmp.*
from {{ ref('STATISTICS_REPORT_AB2') }} tmp
-- STATISTICS_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

