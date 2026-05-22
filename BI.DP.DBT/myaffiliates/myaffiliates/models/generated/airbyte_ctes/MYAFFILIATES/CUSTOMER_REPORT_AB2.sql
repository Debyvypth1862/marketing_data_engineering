{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "MYAFFILIATES",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to try_cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('CUSTOMER_REPORT_AB1') }}
select
    try_cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    try_cast(PAYLOAD as {{ dbt_utils.type_string() }}) as PAYLOAD,
    try_cast(CAMPAIGN as {{ dbt_utils.type_string() }}) as CAMPAIGN,
    try_cast(CAMPAIGN as {{ dbt_utils.type_string() }}) as CAMPAIGN_GROUP,
    CASE 
        WHEN CLICKS IS NOT NULL THEN try_cast(CLICKS as {{ dbt_utils.type_float() }})
        WHEN HITS IS NOT NULL THEN  try_cast(HITS as {{ dbt_utils.type_float() }})
        ELSE NULL
    END as CLICKS,
    try_cast(CUSTOMER as {{ dbt_utils.type_string() }}) as CUSTOMER,
    CASE 
        WHEN TOTAL_DEPOSITS IS NOT NULL THEN try_cast(TOTAL_DEPOSITS as {{ dbt_utils.type_float() }}) 
        WHEN DEPOSITS IS NOT NULL THEN try_cast(DEPOSITS as {{ dbt_utils.type_float() }}) 
        ELSE NUll
    END as DEPOSITS,
    CASE 
        WHEN FIRST_DEPOSIT IS NOT NULL THEN try_cast(FIRST_DEPOSIT as {{ dbt_utils.type_float() }}) 
        WHEN FTD IS NOT NULL THEN try_cast(FTD as {{ dbt_utils.type_float() }}) 
        ELSE NUll
    END as FIRST_DEPOSIT,
    CASE 
        WHEN FIRST_DEPOSIT_COUNT IS NOT NULL THEN try_cast(FIRST_DEPOSIT_COUNT as {{ dbt_utils.type_float() }}) 
        WHEN FTD_COUNT IS NOT NULL THEN try_cast(FTD_COUNT as {{ dbt_utils.type_float() }}) 
        ELSE NUll
    END as FIRST_DEPOSIT_COUNT,
    try_cast(IMPRESSIONS as {{ dbt_utils.type_float() }}) as IMPRESSIONS,
    try_cast(INCOME as {{ dbt_utils.type_float() }}) as INCOME,
    try_cast(MEDIA as {{ dbt_utils.type_string() }}) as MEDIA,
    CASE
        WHEN NET_REVENUE IS NOT NULL  THEN try_cast(NET_REVENUE as {{ dbt_utils.type_float() }})
        WHEN TOTAL_NET_REVENUE IS NOT NULL THEN try_cast(TOTAL_NET_REVENUE as {{ dbt_utils.type_float() }})
        WHEN NGR IS NOT NULL THEN try_cast(NGR as {{ dbt_utils.type_float() }})
        ELSE NULL
    END as NET_REVENUE,
    try_cast(QUALIFIED_PLAYERS as {{ dbt_utils.type_float() }}) as QUALIFIED_PLAYERS,
    try_cast(SIGNUPS as {{ dbt_utils.type_float() }}) as SIGNUPS,
    try_cast(BILLING_TITLE as {{ dbt_utils.type_string() }}) as BILLING_TITLE,
    try_cast(CURRENCY_RATE as {{ dbt_utils.type_float() }}) as CURRENCY_RATE,
    try_cast(CURRENT_SUBSCRIPTION as {{ dbt_utils.type_string() }}) as CURRENT_SUBSCRIPTION,
    try_cast(CUSTOMER_GROUP as {{ dbt_utils.type_string() }}) as CUSTOMER_GROUP,
    try_cast(GROUP_DESCRIPTION as {{ dbt_utils.type_string() }}) as GROUP_DESCRIPTION,
    try_cast(LINEAR as {{ dbt_utils.type_float() }}) as LINEAR,
    try_cast(PLAN_ID as {{ dbt_utils.type_string() }}) as PLAN_ID,
    try_cast(SUB_END_DATE as {{ dbt_utils.type_string() }}) as SUB_END_DATE,
    try_cast(SUBSCRIPTION as {{ dbt_utils.type_string() }}) as SUBSCRIPTION,
    try_cast(SYSTEMCURRENCY as {{ dbt_utils.type_string() }}) as SYSTEMCURRENCY,
    try_cast(NDC as {{ dbt_utils.type_float() }}) as NDC,
    try_cast(BONUSES as {{ dbt_utils.type_float() }}) as BONUSES,
    try_cast(ADMIN_FEE as {{ dbt_utils.type_float() }}) as ADMIN_FEE,
    try_cast(USERCURRENCY as {{ dbt_utils.type_string() }}) as USERCURRENCY,
    try_cast(TOTAL_DEPOSITS as {{ dbt_utils.type_float() }}) as TOTAL_DEPOSITS,
    try_cast(TOTAL_PL as {{ dbt_utils.type_float() }}) as TOTAL_PL,
    try_cast(TOTAL_STAKE as {{ dbt_utils.type_float() }}) as TOTAL_STAKE,
    try_cast(TOTAL_VALID_TURNOVER as {{ dbt_utils.type_float() }}) as TOTAL_VALID_TURNOVER,
    try_cast(NGR as {{ dbt_utils.type_float() }}) as NGR,
    try_cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_string() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CUSTOMER_REPORT_AB1') }}
-- CUSTOMER_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

