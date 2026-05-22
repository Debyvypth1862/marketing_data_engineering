{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('BRANDS_AB2') }}
select
	BRAN_ID,
	BRAN_NAME,
	BRAN_PLATFORM,
	BRAN_SLUG,
	BRAN_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('BRANDS_AB2') }}
-- BRANDS from {{ source('BRC', '_AIRBYTE_RAW_BRANDS') }}
where 1 = 1