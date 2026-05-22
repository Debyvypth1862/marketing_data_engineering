{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "MYAFFILIATES",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('CUSTOMER_REPORT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'DATE',
        'PAYLOAD',
        'CAMPAIGN',
        'CAMPAIGN_GROUP',
        'CLICKS',
        'CUSTOMER',
        'DEPOSITS',
        'FIRST_DEPOSIT',
        'FIRST_DEPOSIT_COUNT',
        'IMPRESSIONS',
        'INCOME',
        'MEDIA',
        'NET_REVENUE',
        'QUALIFIED_PLAYERS',
        'SIGNUPS',
        'BILLING_TITLE',
        'CURRENCY_RATE',
        'CURRENT_SUBSCRIPTION',
        'CUSTOMER_GROUP',
        'GROUP_DESCRIPTION',
        'LINEAR',
        'PLAN_ID',
        'SUB_END_DATE',
        'SUBSCRIPTION',
        'SYSTEMCURRENCY',
        'NDC',
        'BONUSES',
        'ADMIN_FEE',
        'USERCURRENCY',
        'TRACKER_LOGIN_ID',
        'NGR',
        'TOTAL_DEPOSITS',
        'TOTAL_PL',
        'TOTAL_STAKE',
        'TOTAL_VALID_TURNOVER'
    ]) }} as _AIRBYTE_CUSTOMER_REPORT_HASHID,
    tmp.*
from {{ ref('CUSTOMER_REPORT_AB2') }} tmp
-- CUSTOMER_REPORT
where 1 = 1 
AND PAYLOAD is not null
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

