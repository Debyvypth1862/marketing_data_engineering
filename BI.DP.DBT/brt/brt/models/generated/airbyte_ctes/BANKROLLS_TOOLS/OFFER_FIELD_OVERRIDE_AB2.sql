{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OFFER_FIELD_OVERRIDE_AB1') }}
select
    cast(FIELD as {{ dbt_utils.type_string() }}) as FIELD,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(OFFER_ID as {{ dbt_utils.type_bigint() }}) as OFFER_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFER_FIELD_OVERRIDE_AB1') }}
-- OFFER_FIELD_OVERRIDE
where 1 = 1

