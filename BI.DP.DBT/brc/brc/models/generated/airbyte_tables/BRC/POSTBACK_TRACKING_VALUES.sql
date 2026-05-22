{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('POSTBACK_TRACKING_VALUES_AB2') }}
select
	POTV_CLICKID,
	POTV_DATE,
	POTV_DEPOSIT_VALUE,
	POTV_ID,
	POTV_REVSHARE,
	POTV_TIMESTAMP,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_TRACKING_VALUES_AB2') }}
-- POSTBACK_TRACKING_VALUES from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_TRACKING_VALUES') }}
where 1 = 1
