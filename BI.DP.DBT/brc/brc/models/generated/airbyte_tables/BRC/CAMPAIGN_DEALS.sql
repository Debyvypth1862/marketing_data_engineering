{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CAMPAIGN_DEALS_AB2') }}
select
	CADE_CAMP_ARRAY,
	CADE_CREATED,
	CADE_CREATED_BY,
	CADE_DEAL,
	CADE_DEAL_TEXT,
	CADE_DESCRIPTION,
	CADE_FEATURED,
	CADE_FK_ADVERTISER,
	CADE_ID,
	CADE_LANG_ARRAY,
	CADE_LIVE,
	CADE_POSITION,
	CADE_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_DEALS_AB2') }}
-- CAMPAIGN_DEALS from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_DEALS') }}
where 1 = 1