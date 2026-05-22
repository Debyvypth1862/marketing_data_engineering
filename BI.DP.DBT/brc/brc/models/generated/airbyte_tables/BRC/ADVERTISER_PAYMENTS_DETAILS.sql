{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ADVERTISER_PAYMENTS_DETAILS_AB2') }}
select
	ADDE_CREATED,
	ADDE_FK_ADPA_ID,
	ADDE_FK_ADVERTISER,
	ADDE_FK_PAYER,
	ADDE_ID,
	ADDE_INCOME,
	ADDE_UPDATED,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISER_PAYMENTS_DETAILS_AB2') }}
-- ADVERTISER_PAYMENTS_DETAILS from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISER_PAYMENTS_DETAILS') }}
where 1 = 1