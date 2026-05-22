{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ADVERTISER_OPERATOR_STG') }}
select
    
    NOTE,
    UPDATED_AT,
    OPERATOR_ID,
    NAME,
    CREATED_AT,
    ID,
    ADVERTISER_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_ADVERTISER_OPERATOR_HASHID
from {{ ref('ADVERTISER_OPERATOR_STG') }}
-- ADVERTISER_OPERATOR from {{ source('BRT', '_AIRBYTE_RAW_ADVERTISER_OPERATOR') }}
where 1 = 1


