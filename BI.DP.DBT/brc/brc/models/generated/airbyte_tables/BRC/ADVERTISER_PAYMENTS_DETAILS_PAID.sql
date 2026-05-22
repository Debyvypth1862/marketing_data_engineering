{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ADVERTISER_PAYMENTS_DETAILS_PAID_AB2') }}
select
	PAYD_AMOUNT,
	PAYD_COMMENT,
	PAYD_DATE,
	PAYD_FK_ADDE_ID,
	PAYD_FK_ADPA_ID,
	PAYD_ID,
	PAYD_RECEIVED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISER_PAYMENTS_DETAILS_PAID_AB2') }}
-- ADVERTISER_PAYMENTS_DETAILS_PAID from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISER_PAYMENTS_DETAILS_PAID') }}
where 1 = 1