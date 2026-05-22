{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CAMPAIGN_PRODUCTS_AB2') }}
select
	CAPR_CATEGORY,
	CAPR_ID,
	CAPR_NAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_PRODUCTS_AB2') }}
-- CAMPAIGN_PRODUCTS from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_PRODUCTS') }}
where 1 = 1