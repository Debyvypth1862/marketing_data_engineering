{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "SMARTICO",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('UTM_CODE_REPORT_AB1') }}
select
    try_cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    try_cast(ID as {{ dbt_utils.type_string() }}) as ID,
    try_cast(AFP as {{ dbt_utils.type_string() }}) as AFP,
    try_cast(BALANCE as {{ dbt_utils.type_float() }}) as BALANCE,
    try_cast(CHARGBACK_TOTAL as {{ dbt_utils.type_float() }}) as CHARGBACK_TOTAL,
    try_cast(UTM_CAMPAIGN as {{ dbt_utils.type_string() }}) as UTM_CAMPAIGN,
    try_cast(UTM_MEDIUM as {{ dbt_utils.type_string() }}) as UTM_MEDIUM,
    try_cast(UTM_SOURCE as {{ dbt_utils.type_string() }}) as UTM_SOURCE,
    try_cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_string() }}) as TRACKER_LOGIN_ID,
    try_cast(adjustment_affiliate as {{ dbt_utils.type_float() }}) as adjustment_affiliate,
    try_cast(adjustment_registration as {{ dbt_utils.type_float() }}) as adjustment_registration,
    try_cast(adjustments as {{ dbt_utils.type_float() }}) as adjustments,
    try_cast(bonus_amount as {{ dbt_utils.type_float() }}) as bonus_amount,
    try_cast(brand_id as {{ dbt_utils.type_string() }}) as brand_id,
    try_cast(brand_name as {{ dbt_utils.type_string() }}) as brand_name,
    try_cast(chargeback_total as {{ dbt_utils.type_float() }}) as chargeback_total,
    try_cast(commissions_cpa as {{ dbt_utils.type_float() }}) as commissions_cpa,
    try_cast(commissions_cpl as {{ dbt_utils.type_float() }}) as commissions_cpl,
    try_cast(commissions_rev_share as {{ dbt_utils.type_float() }}) as commissions_rev_share,
    try_cast(commissions_total as {{ dbt_utils.type_float() }}) as commissions_total,
    try_cast(conversion_rate as {{ dbt_utils.type_float() }}) as conversion_rate,
    try_cast(deductions as {{ dbt_utils.type_float() }}) as deductions,
    try_cast(deposit_count as {{ dbt_utils.type_float() }}) as deposit_count,
    try_cast(deposit_total as {{ dbt_utils.type_float() }}) as deposit_total,
    try_cast(dt as {{ dbt_utils.type_string() }}) as dt,
    try_cast(ftd_count as {{ dbt_utils.type_float() }}) as ftd_count,
    try_cast(ftd_total as {{ dbt_utils.type_float() }}) as ftd_total,
    try_cast(link_id as {{ dbt_utils.type_string() }}) as link_id,
    try_cast(link_name as {{ dbt_utils.type_string() }}) as link_name,
    try_cast(net_deposit_total as {{ dbt_utils.type_float() }}) as net_deposit_total,
    try_cast(net_deposits as {{ dbt_utils.type_float() }}) as net_deposits,
    try_cast(net_pl as {{ dbt_utils.type_float() }}) as net_pl,
    try_cast(net_pl_casino as {{ dbt_utils.type_float() }}) as net_pl_casino,
    try_cast(net_pl_sport as {{ dbt_utils.type_float() }}) as net_pl_sport,
    try_cast(net_win as {{ dbt_utils.type_float() }}) as net_win,
    try_cast(operations as {{ dbt_utils.type_float() }}) as operations,
    try_cast(payments as {{ dbt_utils.type_float() }}) as payments,
    try_cast(pl as {{ dbt_utils.type_float() }}) as pl,
    try_cast(qftd_count as {{ dbt_utils.type_float() }}) as qftd_count,
    try_cast(qlead_count as {{ dbt_utils.type_float() }}) as qlead_count,
    try_cast(registration_count as {{ dbt_utils.type_float() }}) as registration_count,
    try_cast(sub_commission_from_child as {{ dbt_utils.type_float() }}) as sub_commission_from_child,
    try_cast(visit_count as {{ dbt_utils.type_float() }}) as visit_count,
    try_cast(volume as {{ dbt_utils.type_float() }}) as volume,
    try_cast(withdrawal_count as {{ dbt_utils.type_float() }}) as withdrawal_count,
    try_cast(withdrawal_total as {{ dbt_utils.type_float() }}) as withdrawal_total,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('UTM_CODE_REPORT_AB1') }}
-- UTM_CODE_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
-- AND NET_REVENUE IS NOT NULL
-- AND AFP IS NOT NULL
