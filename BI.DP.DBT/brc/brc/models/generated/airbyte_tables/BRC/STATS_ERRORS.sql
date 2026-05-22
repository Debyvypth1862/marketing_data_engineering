{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('STATS_ERRORS_AB2') }}
select
	STER_BACKUP_DATE,
	STER_CHECKED_TIME,
	STER_FK_ADVERTISER,
	STER_FK_PUBLISHER,
	STER_ID,
	STER_MISSING_JSON,
	STER_MONTH,
	STER_STATUS,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('STATS_ERRORS_AB2') }}
-- STATS_ERRORS from {{ source('BRC', '_AIRBYTE_RAW_STATS_ERRORS') }}
where 1 = 1