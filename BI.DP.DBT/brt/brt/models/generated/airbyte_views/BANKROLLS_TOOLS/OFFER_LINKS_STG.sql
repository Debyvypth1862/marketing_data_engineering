{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFER_LINKS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'LINK_REVIEW',
        'UPDATED_AT',
        'LINK_BANNER',
        'LINK_TERMS',
        'LINK_OFFER',
        'CREATED_AT',
        'ID',
        'MARKETING_SITE_ID',
        'DELETED_AT',
        'OFFER_ID',
        'EXTERNAL_REVIEW_PAGE',
    ]) }} as _AIRBYTE_OFFER_LINKS_HASHID,
    tmp.*
from {{ ref('OFFER_LINKS_AB2') }} tmp
-- OFFER_LINKS
where 1 = 1

