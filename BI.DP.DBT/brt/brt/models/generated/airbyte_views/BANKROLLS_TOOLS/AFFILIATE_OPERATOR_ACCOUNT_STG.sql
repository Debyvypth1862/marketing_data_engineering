{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('AFFILIATE_OPERATOR_ACCOUNT_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'UPDATED_AT',
        'AFFILIATE_ID',
        'CREATED_AT',
        'ID',
        'OPERATOR_ACCOUNT_ID',
    ]) }} as _AIRBYTE_AFFILIATE_OPERATOR_ACCOUNT_HASHID,
    tmp.*
from {{ ref('AFFILIATE_OPERATOR_ACCOUNT_AB2') }} tmp
-- AFFILIATE_OPERATOR_ACCOUNT
where 1 = 1

