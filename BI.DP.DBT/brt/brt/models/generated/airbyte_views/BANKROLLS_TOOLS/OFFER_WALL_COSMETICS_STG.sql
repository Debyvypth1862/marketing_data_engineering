{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFER_WALL_COSMETICS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'OFFER_WALL_ID',
        'PARAMETER',
        'CREATED_AT',
        'ID',
        'VALUE',
        'DELETED_AT',
    ]) }} as _AIRBYTE_OFFER_WALL_COSMETICS_HASHID,
    tmp.*
from {{ ref('OFFER_WALL_COSMETICS_AB2') }} tmp
-- OFFER_WALL_COSMETICS
where 1 = 1

