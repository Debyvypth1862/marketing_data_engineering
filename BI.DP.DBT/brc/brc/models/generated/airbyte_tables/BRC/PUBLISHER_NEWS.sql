{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('PUBLISHER_NEWS_AB2') }}
select
	PUNE_ACTIVE,
	PUNE_CREATED,
	PUNE_CREATED_BY,
	PUNE_HEADLINE,
	PUNE_ID,
	PUNE_IMPORTANT,
	PUNE_STATUS,
	PUNE_TEXT,
	PUNE_THUMB,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PUBLISHER_NEWS_AB2') }}
-- PUBLISHER_NEWS from {{ source('BRC', '_AIRBYTE_RAW_PUBLISHER_NEWS') }}
where 1 = 1