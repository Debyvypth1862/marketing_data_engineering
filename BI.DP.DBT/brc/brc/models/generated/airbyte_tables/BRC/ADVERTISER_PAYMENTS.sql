{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ADVERTISER_PAYMENTS_AB2') }}
select
	ADPA_CREATED,
	ADPA_ID,
	ADPA_MONTH,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISER_PAYMENTS_AB2') }}
-- ADVERTISER_PAYMENTS from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISER_PAYMENTS') }}
where 1 = 1