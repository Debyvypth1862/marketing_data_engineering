{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "SMARTICO",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('UTM_CODE_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([

         'DATE',
         'ID',
        'ADJUSTMENT_AFFILIATE',
        'ADJUSTMENT_REGISTRATION',
        'ADJUSTMENTS',
        'BALANCE',
        'BONUS_AMOUNT',
        'BRAND_ID',
        'BRAND_NAME',
        'CHARGEBACK_TOTAL',
        'COMMISSIONS_CPA',
        'COMMISSIONS_CPL',
        'COMMISSIONS_REV_SHARE',
        'COMMISSIONS_TOTAL',
        'CONVERSION_RATE',
        'DEDUCTIONS',
        'DEPOSIT_COUNT',
        'DEPOSIT_TOTAL',
        'DT',
        'FTD_COUNT',
        'FTD_TOTAL',
        'LINK_ID',
        'LINK_NAME',
        'NET_DEPOSIT_TOTAL',
        'NET_DEPOSITS',
        'NET_PL',
        'NET_PL_CASINO',
        'NET_PL_SPORT',
        'NET_WIN',
        'OPERATIONS',
        'PAYMENTS',
        'PL',
        'QFTD_COUNT',
        'QLEAD_COUNT',
        'REGISTRATION_COUNT',
        'SUB_COMMISSION_FROM_CHILD',
        'VISIT_COUNT',
        'VOLUME',
        'WITHDRAWAL_COUNT',
        'WITHDRAWAL_TOTAL',
        'AFP',
        'CHARGBACK_TOTAL',
        'UTM_CAMPAIGN',
        'UTM_MEDIUM',
        'UTM_SOURCE',
        'TRACKER_LOGIN_ID',
    ]) }} as _AIRBYTE_UTM_CODE_REPORT_HASHID,
    tmp.*
from {{ ref('UTM_CODE_REPORT_AB2') }} tmp
-- UTM_CODE_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

