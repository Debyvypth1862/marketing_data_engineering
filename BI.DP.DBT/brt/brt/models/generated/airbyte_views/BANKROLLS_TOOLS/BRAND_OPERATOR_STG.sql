{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('BRAND_OPERATOR_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'OPERATOR_ID',
        'CREATED_AT',
        'ID',
        'BRAND_ID',
    ]) }} as _AIRBYTE_BRAND_OPERATOR_HASHID,
    tmp.*
from {{ ref('BRAND_OPERATOR_AB2') }} tmp
-- BRAND_OPERATOR
where 1 = 1

