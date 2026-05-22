{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_STATE_AB3') }}
select
    ID,
    STATE_ID,
    OFFER_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_STATE_HASHID
from {{ ref('OFFER_STATE_AB3') }}
-- OFFER_STATE from {{ source('BRT', '_AIRBYTE_RAW_OFFER_STATE') }}
where 1 = 1

