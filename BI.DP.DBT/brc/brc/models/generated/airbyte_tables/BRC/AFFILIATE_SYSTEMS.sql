{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('AFFILIATE_SYSTEMS_AB2') }}
select
	AFSY_ACTIVE,
	AFSY_COLUMNS,
	AFSY_CUSTOM,
	AFSY_ID,
	AFSY_NAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('AFFILIATE_SYSTEMS_AB2') }}
-- AFFILIATE_SYSTEMS from {{ source('BRC', '_AIRBYTE_RAW_AFFILIATE_SYSTEMS') }}
where 1 = 1