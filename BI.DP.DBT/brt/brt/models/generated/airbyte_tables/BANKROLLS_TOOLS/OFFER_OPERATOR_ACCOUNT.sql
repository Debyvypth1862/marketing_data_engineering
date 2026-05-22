{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_OPERATOR_ACCOUNT_STG') }}
select
    
    UPDATED_AT,
    CREATED_AT,
    ID,
    OFFER_ID,
    OPERATOR_ACCOUNT_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_OPERATOR_ACCOUNT_HASHID
from {{ ref('OFFER_OPERATOR_ACCOUNT_STG') }}
-- OFFER_OPERATOR_ACCOUNT from {{ source('BRT', '_AIRBYTE_RAW_OFFER_OPERATOR_ACCOUNT') }}
where 1 = 1


