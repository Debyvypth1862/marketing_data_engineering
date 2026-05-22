{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFER_FIELD_OVERRIDE_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'FIELD',
        'ID',
        'OFFER_ID',
    ]) }} as _AIRBYTE_OFFER_FIELD_OVERRIDE_HASHID,
    tmp.*
from {{ ref('OFFER_FIELD_OVERRIDE_AB2') }} tmp
-- OFFER_FIELD_OVERRIDE
where 1 = 1

