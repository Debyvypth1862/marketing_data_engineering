{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "EGO",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('BRAND_REPORT_AB1') }}
select
    try_cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    try_cast(AFFILIATE as {{ dbt_utils.type_string() }}) as AFFILIATE,
    try_cast(AFFILIATE_REVENUE as {{ dbt_utils.type_float() }}) as AFFILIATE_REVENUE,
    try_cast(CHARGEBACK_QTY as {{ dbt_utils.type_float() }}) as CHARGEBACK_QTY,
    try_cast(COMPLETE_DOWNLOADS as {{ dbt_utils.type_float() }}) as COMPLETE_DOWNLOADS,
    try_cast(CREDIT_QTY as {{ dbt_utils.type_float() }}) as CREDIT_QTY,
    try_cast(DYNID as {{ dbt_utils.type_string() }}) as DYNID,
    try_cast(FIRST_DEPOSITS_QTY as {{ dbt_utils.type_float() }}) as FIRST_DEPOSITS_QTY,
    try_cast(FLAT_FEE as {{ dbt_utils.type_float() }}) as FLAT_FEE,
    try_cast(FRAUD_QTY as {{ dbt_utils.type_float() }}) as FRAUD_QTY,
    try_cast(HITS as {{ dbt_utils.type_float() }}) as HITS,
    try_cast(NET_INCOME as {{ dbt_utils.type_float() }}) as NET_INCOME,
    try_cast(REVENUE_CPA as {{ dbt_utils.type_float() }}) as REVENUE_CPA,
    try_cast(REVENUE_OVERRIDE as {{ dbt_utils.type_float() }}) as REVENUE_OVERRIDE,
    try_cast(REVENUE_SHARE as {{ dbt_utils.type_float() }}) as REVENUE_SHARE,
    try_cast(REVENUE_SUBS as {{ dbt_utils.type_float() }}) as REVENUE_SUBS,
    try_cast(SIGN_UPS as {{ dbt_utils.type_float() }}) as SIGN_UPS,
    try_cast(VALID_SIGN_UPS as {{ dbt_utils.type_float() }}) as VALID_SIGN_UPS,
    try_cast(VOID_QTY as {{ dbt_utils.type_float() }}) as VOID_QTY,
    try_cast(ZONE_ID as {{ dbt_utils.type_string() }}) as ZONE_ID,
    try_cast(REPORT as {{ dbt_utils.type_string() }}) as REPORT,
    try_cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_string() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('BRAND_REPORT_AB1') }}
-- BRAND_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
-- AND NET_REVENUE IS NOT NULL
AND DYNID IS NOT NULL
