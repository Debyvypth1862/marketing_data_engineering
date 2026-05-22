{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('POSTBACK_DOMAINS_AB2') }}
select
	PODO_DOMAIN,
	PODO_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_DOMAINS_AB2') }}
-- POSTBACK_DOMAINS from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_DOMAINS') }}
where 1 = 1