{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CAMPAIGN_MATERIALS_AB2') }}
select
	CAMA_CREATED,
	CAMA_CREATED_BY,
	CAMA_DELETED,
	CAMA_FILE_HEIGHT,
	CAMA_FILE_WIDTH,
	CAMA_FILENAME,
	CAMA_FILETYPE,
	CAMA_FK_CAMPAIGN,
	CAMA_HIDDEN,
	CAMA_ID,
	CAMA_LANG,
	CAMA_NAME,
	CAMA_TEXT_HEADLINE,
	CAMA_TEXT_TEXT,
	CAMA_TYPE,
	CAMA_UPDATED,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CAMPAIGN_MATERIALS_AB2') }}
-- CAMPAIGN_MATERIALS from {{ source('BRC', '_AIRBYTE_RAW_CAMPAIGN_MATERIALS') }}
where 1 = 1