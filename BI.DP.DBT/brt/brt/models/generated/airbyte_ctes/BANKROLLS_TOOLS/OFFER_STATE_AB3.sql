{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "_AIRBYTE_BANKROLLS_TOOLS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFER_STATE_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'ID',
        'STATE_ID',
        'OFFER_ID',
    ]) }} as _AIRBYTE_OFFER_STATE_HASHID,
    tmp.*
from {{ ref('OFFER_STATE_AB2') }} tmp
-- OFFER_STATE
where 1 = 1

