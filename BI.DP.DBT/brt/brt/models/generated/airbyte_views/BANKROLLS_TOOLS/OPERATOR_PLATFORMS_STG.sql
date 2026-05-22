{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OPERATOR_PLATFORMS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'NOTE',
        'POSTBACK',
        'URL_LOGO',
        'UPDATED_AT',
        'NAME',
        'CREATED_AT',
        boolean_to_string('HAS_API'),
        'ID',
        'API_DOCUMENTATION_URL',
        'URL',
        'HAS_PLAYER_LEVEL_DATA',
    ]) }} as _AIRBYTE_OPERATOR_PLATFORMS_HASHID,
    tmp.*
from {{ ref('OPERATOR_PLATFORMS_AB2') }} tmp
-- OPERATOR_PLATFORMS
where 1 = 1

