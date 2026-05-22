{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('PAYMENTS_AB2') }}
select
	PAYM_CREATED,
	PAYM_CREATED_BY,
	PAYM_FK_PUBLISHER,
	PAYM_ID,
	PAYM_NOTE,
	PAYM_PAYMENT_INFO,
	PAYM_PERIOD_FROM,
	PAYM_PERIOD_TO,
	PAYM_STATUS,
	PAYM_UPDATED,
	PAYM_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PAYMENTS_AB2') }}
-- PAYMENTS from {{ source('BRC', '_AIRBYTE_RAW_PAYMENTS') }}
where 1 = 1