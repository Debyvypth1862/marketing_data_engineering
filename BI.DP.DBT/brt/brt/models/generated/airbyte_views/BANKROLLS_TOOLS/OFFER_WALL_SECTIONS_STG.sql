{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFER_WALL_SECTIONS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'FOOTER_SCRIPT',
        'CLOAKER_CONFIGURATION_ID',
        'CREATED_AT',
        'MARKETING_SITE_ID',
        boolean_to_string('PARAMS'),
        'UUID',
        'CREATED_BY',
        'DELETED_AT',
        'UPDATED_AT',
        'AFTER_BODY_SCRIPT',
        'USER_ID',
        'NAME',
        'UPDATED_BY',
        'HEADER_SCRIPT',
        'ID',
    ]) }} as _AIRBYTE_OFFER_WALL_SECTIONS_HASHID,
    tmp.*
from {{ ref('OFFER_WALL_SECTIONS_AB2') }} tmp
-- OFFER_WALL_SECTIONS
where 1 = 1

