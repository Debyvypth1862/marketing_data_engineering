{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CAMPAIGN_TRACKER_DEALS_AB2') }}
select
	CATD_CPA_IN,
	CATD_CPA_OUT,
	CATD_CPL_IN,
	CATD_CPL_OUT,
	CATD_DISPLAY_DEAL,
	CATD_FK_CAMT_ID,
	CATD_ID,
	CATD_REV_IN,
	CATD_REV_OUT,
	CATD_START_MONTH,
	CATD_UPDATED,
	CATD_UPDATED_BY,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_TRACKER_DEALS_AB2') }}
-- CAMPAIGN_TRACKER_DEALS from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_TRACKER_DEALS') }}
where 1 = 1