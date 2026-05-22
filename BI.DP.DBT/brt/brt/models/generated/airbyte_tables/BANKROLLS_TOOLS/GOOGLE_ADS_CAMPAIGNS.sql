{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('GOOGLE_ADS_CAMPAIGNS_STG') }}
select
    
    NOTES,
    BUDGET_TIMING,
    CAMPAIGN_TYPE,
    CREATED_AT,
    CREATED_BY,
    DELETED_AT,
    CURRENCY_CODE,
    GOOGLE_ADS_ACCOUNT_ID,
    UPDATED_AT,
    NAME,
    UPDATED_BY,
    ID,
    CAMPAIGN_ID,
    DATA_COLLECTION_METHOD,
    STATUS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_GOOGLE_ADS_CAMPAIGNS_HASHID
from {{ ref('GOOGLE_ADS_CAMPAIGNS_STG') }}
-- GOOGLE_ADS_CAMPAIGNS from {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_CAMPAIGNS') }}
where 1 = 1


