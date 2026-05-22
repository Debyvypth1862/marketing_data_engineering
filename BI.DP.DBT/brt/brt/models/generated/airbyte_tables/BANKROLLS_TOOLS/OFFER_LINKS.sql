{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OFFER_LINKS_STG') }}
select
    
    LINK_REVIEW,
    UPDATED_AT,
    LINK_BANNER,
    LINK_TERMS,
    LINK_OFFER,
    CREATED_AT,
    ID,
    MARKETING_SITE_ID,
    DELETED_AT,
    OFFER_ID,
    EXTERNAL_REVIEW_PAGE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFER_LINKS_HASHID
from {{ ref('OFFER_LINKS_STG') }}
-- OFFER_LINKS from {{ source('BRT', '_AIRBYTE_RAW_OFFER_LINKS') }}
where 1 = 1


