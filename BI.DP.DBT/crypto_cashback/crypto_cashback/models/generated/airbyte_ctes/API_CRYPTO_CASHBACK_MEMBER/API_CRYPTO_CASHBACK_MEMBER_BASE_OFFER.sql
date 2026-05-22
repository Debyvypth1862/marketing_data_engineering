{{ config(
    materialized = "ephemeral"
) }}
-- Get deduplicated offer records with latest version
Select
    off.ID,
    off.TRACKING_ID,
    off.Title as Offer_Name,
    off.Deleted
from {{ source('BRT', 'OFFERS') }} off
join {{ ref('API_CRYPTO_CASHBACK_MEMBER_BASE_OFFER_LATEST') }} ls
    on off.TRACKING_ID = ls.TRACKING_ID
    and off.updated_at = ls.updated_at
    and off._airbyte_emitted_at = ls._airbyte_emitted_at
where off.Deleted = 'FALSE'
Group By All
