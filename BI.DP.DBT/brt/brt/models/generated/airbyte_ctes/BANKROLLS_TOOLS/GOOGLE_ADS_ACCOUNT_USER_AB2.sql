{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('GOOGLE_ADS_ACCOUNT_USER_AB1') }}
select
    cast(USER_ID as {{ dbt_utils.type_bigint() }}) as USER_ID,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(GOOGLE_ADS_ACCOUNT_ID as {{ dbt_utils.type_bigint() }}) as GOOGLE_ADS_ACCOUNT_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('GOOGLE_ADS_ACCOUNT_USER_AB1') }}
-- GOOGLE_ADS_ACCOUNT_USER
where 1 = 1

