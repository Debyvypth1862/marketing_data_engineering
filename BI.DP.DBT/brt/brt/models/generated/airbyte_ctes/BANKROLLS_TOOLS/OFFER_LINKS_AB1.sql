{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}


-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('BRT', '_AIRBYTE_RAW_OFFER_LINKS') }}
select
    {{ json_extract_scalar('_airbyte_data', ['link_review'], ['link_review']) }} as LINK_REVIEW,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as UPDATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['link_banner'], ['link_banner']) }} as LINK_BANNER,
    {{ json_extract_scalar('_airbyte_data', ['link_terms'], ['link_terms']) }} as LINK_TERMS,
    {{ json_extract_scalar('_airbyte_data', ['link_offer'], ['link_offer']) }} as LINK_OFFER,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as CREATED_AT,
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as ID,
    {{ json_extract_scalar('_airbyte_data', ['marketing_site_id'], ['marketing_site_id']) }} as MARKETING_SITE_ID,
    {{ json_extract_scalar('_airbyte_data', ['deleted_at'], ['deleted_at']) }} as DELETED_AT,
    {{ json_extract_scalar('_airbyte_data', ['offer_id'], ['offer_id']) }} as OFFER_ID,
    {{ json_extract_scalar('_airbyte_data', ['external_review_page'], ['external_review_page']) }} as EXTERNAL_REVIEW_PAGE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRT', '_AIRBYTE_RAW_OFFER_LINKS') }} as table_alias
-- OFFER_LINKS
where 1 = 1

