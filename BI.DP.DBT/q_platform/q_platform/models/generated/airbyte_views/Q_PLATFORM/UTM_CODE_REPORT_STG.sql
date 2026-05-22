{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "Q_PLATFORM",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('UTM_CODE_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'START_DATE',
        'END_DATE',
        'MERCHANT',
        'AFFILIATE_ID',
        'AN_ID',
        'ANID1',
        'ANID2',
        'ANID3',
        'ANID4',
        'ANID5',
        'CPA_PROFIT',
        'CPL_PROFIT',
        'CREATIVE_ID',
        'DEPOSITS',
        'GGR',
        'MERCHANT_NAME',
        'NGR',
        'PROFIT',
        'REVENUE_SHARE_PROFIT',
        'SERIAL_ID',
        'SITE_ID',
        'TRANSACTION_DATE',
        'WITHDRAWALS',
        'TRACKER_LOGIN_ID',
    ]) }} as _AIRBYTE_UTM_CODE_REPORT_HASHID,
    tmp.*
from {{ ref('UTM_CODE_REPORT_AB2') }} tmp
-- UTM_CODE_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

