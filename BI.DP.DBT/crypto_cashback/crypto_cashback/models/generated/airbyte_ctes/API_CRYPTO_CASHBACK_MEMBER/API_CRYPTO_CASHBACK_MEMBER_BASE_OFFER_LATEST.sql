{{ config(
    materialized = "ephemeral"
) }}
-- Get latest offer records by TRACKING_ID for deduplication
Select
    TRACKING_ID,
    max(updated_at) as updated_at,
    max(_airbyte_emitted_at) as _airbyte_emitted_at
from {{ source('BRT', 'OFFERS') }}
group by all
