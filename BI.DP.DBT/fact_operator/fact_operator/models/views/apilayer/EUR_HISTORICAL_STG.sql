{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "APILAYER",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('EUR_HISTORICAL_AB2') }}
SELECT
    {{ dbt_utils.surrogate_key([
        'DATE',
        'CURRENCY',
        'RATE',
    ]) }} AS _AIRBYTE_EUR_HISTORICAL_HASHID
  , tmp.*
FROM {{ ref('EUR_HISTORICAL_AB2') }} AS tmp
-- EUR_HISTORICAL
WHERE 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
