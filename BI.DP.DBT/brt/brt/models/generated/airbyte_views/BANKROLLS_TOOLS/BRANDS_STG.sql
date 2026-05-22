{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('BRANDS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'NOTES',
        'URL_LOGO',
        'UPDATED_AT',
        'NAME',
        'UPDATED_BY',
        'CREATED_AT',
        'ID',
        'TYPE',
        'CREATED_BY',
        'URL',
        'SLUG',
    ]) }} as _AIRBYTE_BRANDS_HASHID,
    tmp.*
from {{ ref('BRANDS_AB2') }} tmp
-- BRANDS
where 1 = 1

