{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CAMPAIGN_MATERIALS_REQUEST_NEW_AB2') }}
select
	CARE_DATE,
	CARE_FK_CAMA_ID,
	CARE_FK_PUBLISHER,
	CARE_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_MATERIALS_REQUEST_NEW_AB2') }}
-- CAMPAIGN_MATERIALS_REQUEST_NEW from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_MATERIALS_REQUEST_NEW') }}
where 1 = 1