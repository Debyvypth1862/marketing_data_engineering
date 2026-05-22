{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('LANGUAGES_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'NAME',
        'UPDATED_BY',
        'CREATED_AT',
        'ID',
        'ABBREV',
        'CREATED_BY',
    ]) }} as _AIRBYTE_LANGUAGES_HASHID,
    tmp.*
from {{ ref('LANGUAGES_AB2') }} tmp
-- LANGUAGES
where 1 = 1

