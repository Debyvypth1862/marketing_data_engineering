{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('CURRENCIES_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'SYMBOL',
        'UPDATED_AT',
        'NAME',
        'UPDATED_BY',
        'CREATED_AT',
        'DESCRIPTION',
        'ID',
        'ABBREV',
        'CREATED_BY',
    ]) }} as _AIRBYTE_CURRENCIES_HASHID,
    tmp.*
from {{ ref('CURRENCIES_AB2') }} tmp
-- CURRENCIES
where 1 = 1

