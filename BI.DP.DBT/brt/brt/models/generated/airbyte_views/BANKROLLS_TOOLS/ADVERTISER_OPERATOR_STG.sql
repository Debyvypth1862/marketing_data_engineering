{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('ADVERTISER_OPERATOR_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'NOTE',
        'UPDATED_AT',
        'OPERATOR_ID',
        'NAME',
        'CREATED_AT',
        'ID',
        'ADVERTISER_ID',
    ]) }} as _AIRBYTE_ADVERTISER_OPERATOR_HASHID,
    tmp.*
from {{ ref('ADVERTISER_OPERATOR_AB2') }} tmp
-- ADVERTISER_OPERATOR
where 1 = 1

