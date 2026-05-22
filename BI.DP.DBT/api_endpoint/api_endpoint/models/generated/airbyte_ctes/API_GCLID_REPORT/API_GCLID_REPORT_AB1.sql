{{ config(
    materialized = "ephemeral"
) }}
-- AB1: Add unique identifier based on primary key
select
    {{ dbt_utils.surrogate_key([
        "GCLID",
        "Click_Date",
        "Conversion_Name"
    ]) }} as _AIRBYTE_AB_ID,
    {{ dbt_utils.surrogate_key([
        "GCLID",
        "Click_Date",
        "Conversion_Name"
    ]) }} as _AIRBYTE_UNIQUE_KEY,
    *
from {{ ref('API_GCLID_BASE_CONSOLIDATION') }}
