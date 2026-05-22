{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('PUBLISHER_MANAGERS_AB2') }}
select
	PUMA_FK_ADMIN,
	PUMA_FK_PUBLISHER,
	PUMA_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PUBLISHER_MANAGERS_AB2') }}
-- PUBLISHER_MANAGERS from {{ source('BRC', '_AIRBYTE_RAW_PUBLISHER_MANAGERS') }}
where 1 = 1