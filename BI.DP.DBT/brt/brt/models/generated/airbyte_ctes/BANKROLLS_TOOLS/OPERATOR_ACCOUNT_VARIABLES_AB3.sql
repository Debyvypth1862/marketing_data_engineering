{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OPERATOR_ACCOUNT_VARIABLES_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'ID',
        'VALUE',
        'KEY',
        'OPERATOR_ACCOUNT_ID',
    ]) }} as _AIRBYTE_OPERATOR_ACCOUNT_VARIABLES_HASHID,
    tmp.*
from {{ ref('OPERATOR_ACCOUNT_VARIABLES_AB2') }} tmp
-- OPERATOR_ACCOUNT_VARIABLES
where 1 = 1

