{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "Q_PLATFORM",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('UTM_CODE_REPORT_AB1') }}
select
    try_cast(START_DATE as {{ dbt_utils.type_string() }}) as START_DATE,
    try_cast(END_DATE as {{ dbt_utils.type_string() }}) as END_DATE,
    try_cast(MERCHANT as {{ dbt_utils.type_string() }}) as MERCHANT,
    try_cast(AFFILIATE_ID as {{ dbt_utils.type_int() }}) as AFFILIATE_ID,
    try_cast(AN_ID as {{ dbt_utils.type_string() }}) as AN_ID,
    try_cast(ANID1 as {{ dbt_utils.type_string() }}) as ANID1,
    try_cast(ANID2 as {{ dbt_utils.type_string() }}) as ANID2,
    try_cast(ANID3 as {{ dbt_utils.type_string() }}) as ANID3,
    try_cast(ANID4 as {{ dbt_utils.type_string() }}) as ANID4,
    try_cast(ANID5 as {{ dbt_utils.type_string() }}) as ANID5,
    try_cast(CPA_PROFIT as {{ dbt_utils.type_float() }}) as CPA_PROFIT,
    try_cast(CPL_PROFIT as {{ dbt_utils.type_float() }}) as CPL_PROFIT,
    try_cast(CREATIVE_ID as {{ dbt_utils.type_int() }}) as CREATIVE_ID,
    try_cast(DEPOSITS as {{ dbt_utils.type_float() }}) as DEPOSITS,
    try_cast(GGR as {{ dbt_utils.type_float() }}) as GGR,
    try_cast(MERCHANT_NAME as {{ dbt_utils.type_string() }}) as MERCHANT_NAME,
    try_cast(NGR as {{ dbt_utils.type_float() }}) as NGR,
    try_cast(PROFIT as {{ dbt_utils.type_float() }}) as PROFIT,
    try_cast(REVENUE_SHARE_PROFIT as {{ dbt_utils.type_float() }}) as REVENUE_SHARE_PROFIT,
    try_cast(SERIAL_ID as {{ dbt_utils.type_int() }}) as SERIAL_ID,
    try_cast(SITE_ID as {{ dbt_utils.type_int() }}) as SITE_ID,
    try_cast(TRANSACTION_DATE as {{ dbt_utils.type_string() }}) as TRANSACTION_DATE,
    try_cast(WITHDRAWALS as {{ dbt_utils.type_float() }}) as WITHDRAWALS,
    try_cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_string() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('UTM_CODE_REPORT_AB1') }}
-- UTM_CODE_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
-- AND NET_REVENUE IS NOT NULL
AND AN_ID IS NOT NULL
