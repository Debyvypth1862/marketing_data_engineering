{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CAMPAIGN_DEAL_REQUESTS_AB2') }}
select
	CDRE_ACCEPTED_SEEN,
	CDRE_FK_CAMP_DEAL,
	CDRE_FK_PUBLISHER,
	CDRE_ID,
	CDRE_NOTE,
	CDRE_REQUEST_DATE,
	CDRE_STATUS,
	CDRE_UPDATE_DATE,
	CDRE_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_DEAL_REQUESTS_AB2') }}
-- CAMPAIGN_DEAL_REQUESTS from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_DEAL_REQUESTS') }}
where 1 = 1