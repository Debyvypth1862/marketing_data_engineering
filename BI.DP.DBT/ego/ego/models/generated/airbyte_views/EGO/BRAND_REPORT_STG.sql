{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "EGO",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('BRAND_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'AFFILIATE',
        'AFFILIATE_REVENUE',
        'CHARGEBACK_QTY',
        'COMPLETE_DOWNLOADS',
        'CREDIT_QTY',
        'DATE',
        'DYNID',
        'FIRST_DEPOSITS_QTY',
        'FLAT_FEE',
        'FRAUD_QTY',
        'HITS',
        'NET_INCOME',
        'REVENUE_CPA',
        'REVENUE_OVERRIDE',
        'REVENUE_SHARE',
        'REVENUE_SUBS',
        'SIGN_UPS',
        'VALID_SIGN_UPS',
        'VOID_QTY',
        'ZONE_ID',
        'REPORT',
        'TRACKER_LOGIN_ID',
    ]) }} as _AIRBYTE_BRAND_REPORT_HASHID,
    tmp.*
from {{ ref('BRAND_REPORT_AB2') }} tmp
-- BRAND_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

