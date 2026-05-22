{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "INCOME_ACCESS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('ACCOUNT_REPORT_AB1') }}
select
    cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    cast(DEPOSITS as {{ dbt_utils.type_float() }}) as DEPOSITS,
    cast(COMMISSIONS as {{ dbt_utils.type_float() }}) as COMMISSIONS,
    cast(BONUS as {{ dbt_utils.type_float() }}) as BONUS,
    cast(CPA_COMMISSIONS as {{ dbt_utils.type_float() }}) as CPA_COMMISSIONS,
    cast(CHARGEBACKS as {{ dbt_utils.type_float() }}) as CHARGEBACKS,
    cast(GROSS_REVENUE as {{ dbt_utils.type_float() }}) as GROSS_REVENUE,
    cast(NET_REVENUE as {{ dbt_utils.type_float() }}) as NET_REVENUE,
    cast(AFF_CUSTOM_ID as {{ dbt_utils.type_string() }}) as AFF_CUSTOM_ID,
    cast(BANNER_ID as {{ dbt_utils.type_float() }}) as BANNER_ID,
    cast(BANNER_TYPE as {{ dbt_utils.type_string() }}) as BANNER_TYPE,
    cast(CPA_COMMISSION_COUNT as {{ dbt_utils.type_float() }}) as CPA_COMMISSION_COUNT,
    cast(CREATIVE_NAME as {{ dbt_utils.type_string() }}) as CREATIVE_NAME,
    cast(CURRENCY_SYMBOL as {{ dbt_utils.type_string() }}) as CURRENCY_SYMBOL,
    cast(FIRST_DEPOSIT as {{ dbt_utils.type_string() }}) as FIRST_DEPOSIT,
    cast(MEMBER_ID as {{ dbt_utils.type_float() }}) as MEMBER_ID,
    cast(MERCHANT_NAME as {{ dbt_utils.type_string() }}) as MERCHANT_NAME,
    cast(NEW as {{ dbt_utils.type_float() }}) as NEW,
    cast(PLAYER_ID as {{ dbt_utils.type_float() }}) as PLAYER_ID,
    cast(PLAYER_COUNTRY as {{ dbt_utils.type_string() }}) as PLAYER_COUNTRY,
    cast(REGISTRATION_DATE as {{ dbt_utils.type_string() }}) as REGISTRATION_DATE,
    cast(ROW_ID as {{ dbt_utils.type_float() }}) as ROW_ID,
    cast(SITE_ID as {{ dbt_utils.type_float() }}) as SITE_ID,
    cast(STAKE as {{ dbt_utils.type_float() }}) as STAKE,
    cast(TOTAL_COMMISSION as {{ dbt_utils.type_float() }}) as TOTAL_COMMISSION,
    cast(TOTAL_RECORDS as {{ dbt_utils.type_float() }}) as TOTAL_RECORDS,
    cast(USERNAME as {{ dbt_utils.type_string() }}) as USERNAME,
    cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_float() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ACCOUNT_REPORT_AB1') }}
-- ACCOUNT_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

