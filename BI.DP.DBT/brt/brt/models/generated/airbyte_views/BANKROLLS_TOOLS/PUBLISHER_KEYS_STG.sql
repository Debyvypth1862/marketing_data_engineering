{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('PUBLISHER_KEYS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'USER_ID',
        'NAME',
        'UPDATED_BY',
        'CREATED_AT',
        'ID',
        'TYPE',
        'CREATED_BY',
        'SLUG',
        'REMARKS',
        'TOKEN',
    ]) }} as _AIRBYTE_PUBLISHER_KEYS_HASHID,
    tmp.*
from {{ ref('PUBLISHER_KEYS_AB2') }} tmp
-- PUBLISHER_KEYS
where 1 = 1

