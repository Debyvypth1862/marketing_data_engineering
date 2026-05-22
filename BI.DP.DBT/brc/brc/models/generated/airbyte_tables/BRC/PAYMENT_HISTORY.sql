{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('PAYMENT_HISTORY_AB2') }}
select
	PAHI_FK_PAYMENT,
	PAHI_ID,
	PAHI_TABLE_PAYMENT_DETAILS,
	PAHI_TABLE_PAYMENTS,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PAYMENT_HISTORY_AB2') }}
-- PAYMENT_HISTORY from {{ source('BRC', '_AIRBYTE_RAW_PAYMENT_HISTORY') }}
where 1 = 1