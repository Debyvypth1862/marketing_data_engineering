{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ADVERTISER_PAYERS_AB2') }}
select
	PAYR_ADDRESS,
	PAYR_ADVERTISERS_ARRAY,
	PAYR_CITY,
	PAYR_CONTACT,
	PAYR_COUNTRY,
	PAYR_CREATED,
	PAYR_CREATED_BY,
	PAYR_ID,
	PAYR_NAME,
	PAYR_NOTE,
	PAYR_PAYMENT_AGREEMENT,
	PAYR_PAYMENT_PROVIDER,
	PAYR_REQUEST_DATE,
	PAYR_REQUEST_PAYMENT,
	PAYR_UPDATED,
	PAYR_UPDATED_BY,
	PAYR_VAT,
	PAYR_XERO_ID,
	PAYR_ZIP,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISER_PAYERS_AB2') }}
-- ADVERTISER_PAYERS from {{ source('BRC', '_AIRBYTE_RAW_ADVERTISER_PAYERS') }}
where 1 = 1