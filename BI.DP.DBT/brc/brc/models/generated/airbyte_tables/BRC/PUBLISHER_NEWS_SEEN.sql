{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('PUBLISHER_NEWS_SEEN_AB2') }}
select
	NESE_FK_NEWS,
	NESE_FK_PUBLISHER,
	NESE_ID,
	NESE_SEEN,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PUBLISHER_NEWS_SEEN_AB2') }}
-- PUBLISHER_NEWS_SEEN from {{ source('BRC', '_AIRBYTE_RAW_PUBLISHER_NEWS_SEEN') }}
where 1 = 1