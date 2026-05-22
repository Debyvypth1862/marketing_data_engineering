{{ config(
    materialized = "ephemeral"
) }}
-- AB1: Add unique identifier based on primary key
select
    {{ dbt_utils.surrogate_key([
        "Date",
        "ClickID"
    ]) }} as _AIRBYTE_AB_ID,
    {{ dbt_utils.surrogate_key([
        "Date",
        "ClickID"
    ]) }} as _AIRBYTE_UNIQUE_KEY,
    *
from {{ ref('API_TRAFFICX_BASE_CONSOLIDATION') }}
