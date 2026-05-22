{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('COUNTRY_CURRENCY_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'CREATED_AT',
        'ID',
        'COUNTRY_ID',
        'CURRENCY_ID',
    ]) }} as _AIRBYTE_COUNTRY_CURRENCY_HASHID,
    tmp.*
from {{ ref('COUNTRY_CURRENCY_AB2') }} tmp
-- COUNTRY_CURRENCY
where 1 = 1

