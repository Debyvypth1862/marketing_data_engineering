{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFER_WALL_GEOS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'USER_ID',
        'NAME',
        'UPDATED_BY',
        'CREATED_AT',
        'ID',
        'UUID',
        'CREATED_BY',
        'DELETED_AT',
        'DEFAULT_OFFER_WALL_ID',
    ]) }} as _AIRBYTE_OFFER_WALL_GEOS_HASHID,
    tmp.*
from {{ ref('OFFER_WALL_GEOS_AB2') }} tmp
-- OFFER_WALL_GEOS
where 1 = 1

