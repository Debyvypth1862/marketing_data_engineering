{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('CURRENCY_OPERATOR_ACCOUNT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'CREATED_AT',
        'ID',
        'CURRENCY_ID',
        'OPERATOR_ACCOUNT_ID',
    ]) }} as _AIRBYTE_CURRENCY_OPERATOR_ACCOUNT_HASHID,
    tmp.*
from {{ ref('CURRENCY_OPERATOR_ACCOUNT_AB2') }} tmp
-- CURRENCY_OPERATOR_ACCOUNT
where 1 = 1

