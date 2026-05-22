{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('API_ACCESS_AB2') }}
select
	API_ID,
	API_KEY,
	API_WHITELIST_JSON,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('API_ACCESS_AB2') }}
-- API_ACCESS from {{ source('BRC', '_AIRBYTE_RAW_API_ACCESS') }}
where 1 = 1