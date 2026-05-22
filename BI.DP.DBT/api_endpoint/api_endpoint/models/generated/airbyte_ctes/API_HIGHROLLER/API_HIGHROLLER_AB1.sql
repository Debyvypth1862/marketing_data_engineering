{{ config(
    materialized = "ephemeral"
) }}
-- AB1: Add unique identifier based on primary key
select
    {{ dbt_utils.surrogate_key([
        "date",
        "clickid"
    ]) }} as _AIRBYTE_AB_ID,
    {{ dbt_utils.surrogate_key([
        "date",
        "clickid"
    ]) }} as _AIRBYTE_UNIQUE_KEY,
    *
from {{ ref('API_HIGHROLLER_BASE_CONSOLIDATION') }}