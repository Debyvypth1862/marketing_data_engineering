{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('IP_BLOCK_AB2') }}
select
        BLOCK_ID,
        BLOCK_IP,
        BLOCK_TIMESTAMP,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('IP_BLOCK_AB2') }}
-- IP_BLOCK from {{ source('BRC', '_AIRBYTE_RAW_IP_BLOCK') }}
where 1 = 1