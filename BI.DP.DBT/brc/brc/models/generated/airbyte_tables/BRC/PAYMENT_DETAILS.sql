{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('PAYMENT_DETAILS_AB2') }}
select
	PADE_AMOUNT,
	PADE_DESCRIPTION,
	PADE_FK_PAYMENT,
	PADE_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PAYMENT_DETAILS_AB2') }}
-- PAYMENT_DETAILS from {{ source('BRC', '_AIRBYTE_RAW_PAYMENT_DETAILS') }}
where 1 = 1