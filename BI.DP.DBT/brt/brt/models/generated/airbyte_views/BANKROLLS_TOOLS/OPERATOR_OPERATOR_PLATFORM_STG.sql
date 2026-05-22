{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OPERATOR_OPERATOR_PLATFORM_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'OPERATOR_ID',
        'OPERATOR_PLATFORM_ID',
        'CREATED_AT',
        'ID',
    ]) }} as _AIRBYTE_OPERATOR_OPERATOR_PLATFORM_HASHID,
    tmp.*
from {{ ref('OPERATOR_OPERATOR_PLATFORM_AB2') }} tmp
-- OPERATOR_OPERATOR_PLATFORM
where 1 = 1

