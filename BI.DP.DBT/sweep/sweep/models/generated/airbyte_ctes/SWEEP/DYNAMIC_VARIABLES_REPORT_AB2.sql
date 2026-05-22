{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "SWEEP",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('DYNAMIC_VARIABLES_REPORT_AB1') }}
select
    cast(DATE as {{ dbt_utils.type_string() }}) as DATE,
    cast(DEPOSITS as {{ dbt_utils.type_float() }}) as DEPOSITS,
    cast(WITHDRAWALS as {{ dbt_utils.type_float() }}) as WITHDRAWALS,
    cast(AFP as {{ dbt_utils.type_string() }}) as AFP,
    cast(NET_DEPOSITS as {{ dbt_utils.type_float() }}) as NET_DEPOSITS,
    cast(USERID as {{ dbt_utils.type_string() }}) as USERID,
    cast(COMMISSIONS as {{ dbt_utils.type_float() }}) as COMMISSIONS,
    cast(BRAND as {{ dbt_utils.type_string() }}) as BRAND,
    cast(VOLUME as {{ dbt_utils.type_float() }}) as VOLUME,
    cast(DEPOSIT_COUNT as {{ dbt_utils.type_float() }}) as DEPOSIT_COUNT,
    cast(COMMISSION_COUNT as {{ dbt_utils.type_float() }}) as COMMISSION_COUNT,
    cast(POSITION_COUNT as {{ dbt_utils.type_float() }}) as POSITION_COUNT,
    cast(PL as {{ dbt_utils.type_float() }}) as PL,
    cast(TRACKING_CODE as {{ dbt_utils.type_string() }}) as TRACKING_CODE,
    cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_float() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('DYNAMIC_VARIABLES_REPORT_AB1') }}
-- DYNAMIC_VARIABLES_REPORT_STREAM
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

