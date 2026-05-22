{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('GOOGLE_ADS_CAMPAIGN_USER_AB3') }}
select
    USER_ID,
    GOOGLE_ADS_CAMPAIGN_ID,
    ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_GOOGLE_ADS_CAMPAIGN_USER_HASHID
from {{ ref('GOOGLE_ADS_CAMPAIGN_USER_AB3') }}
-- GOOGLE_ADS_CAMPAIGN_USER from {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_CAMPAIGN_USER') }}
where 1 = 1

