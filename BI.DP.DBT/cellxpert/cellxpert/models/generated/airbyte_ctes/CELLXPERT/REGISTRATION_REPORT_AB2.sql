{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "CELLXPERT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('REGISTRATION_REPORT_AB1') }}
select
    cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    cast(COMMISSIONS as {{ dbt_utils.type_float() }}) as COMMISSIONS,
    cast(COMMISSION as {{ dbt_utils.type_float() }}) as COMMISSION,
    cast(COUNTRY as {{ dbt_utils.type_string() }}) as COUNTRY,
    cast(DEPOSIT_COUNT as {{ dbt_utils.type_float() }}) as DEPOSIT_COUNT,
    cast(DEPOSITS as {{ dbt_utils.type_float() }}) as DEPOSITS,
    cast(EXTERNAL_DATE as {{ dbt_utils.type_string() }}) as EXTERNAL_DATE,
    cast(FIRST_DEPOSIT as {{ dbt_utils.type_float() }}) as FIRST_DEPOSIT,
    cast(FIRST_DEPOSIT_DATE as {{ dbt_utils.type_string() }}) as FIRST_DEPOSIT_DATE,
    cast(NET_DEPOSITS as {{ dbt_utils.type_float() }}) as NET_DEPOSITS,
    cast(PL as {{ dbt_utils.type_float() }}) as PL,
    cast(QUALIFICATION_DATE as {{ dbt_utils.type_string() }}) as QUALIFICATION_DATE,
    cast(REGISTRATION_DATE as {{ dbt_utils.type_string() }}) as REGISTRATION_DATE,
    cast(STATUS as {{ dbt_utils.type_string() }}) as STATUS,
    cast(TRACKING_CODE as {{ dbt_utils.type_string() }}) as TRACKING_CODE,
    cast(USERID as {{ dbt_utils.type_string() }}) as USERID,
    cast(WITHDRAWALS as {{ dbt_utils.type_float() }}) as WITHDRAWALS,
    cast(AFP as {{ dbt_utils.type_string() }}) as AFP,
    cast(GENERIC_1 as {{ dbt_utils.type_string() }}) as GENERIC_1,
    cast(GENERIC_2 as {{ dbt_utils.type_string() }}) as GENERIC_2,
    cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_float() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('REGISTRATION_REPORT_AB1') }}
-- REGISTRATION_REPORT_STREAM
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

