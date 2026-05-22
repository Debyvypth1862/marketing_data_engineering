{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('POSTBACK_3RD_PARTY_CLICK_LOG_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
	POST_3RD_PARTY_CLICKID,
	POST_CLICK_HTTPCODE,
	POST_CLICK_TIMESTAMP,
	POST_CLICK_URL,
	POST_CLICKID,
	POST_FK_CAMT_ID,
	POST_FK_TRACKER,
	POST_ID,
	POST_SUBID,
	POST_SUBID_2,
	POST_SUBID_3,
	POST_SUBID_4,
	POST_SUBID_5,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	S3_PATH,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
	_AIRBYTE_POSTBACK_3RD_PARTY_CLICK_LOG_HASHID
from {{ ref('POSTBACK_3RD_PARTY_CLICK_LOG_SCD') }}
-- POSTBACK_TRACKING from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_3RD_PARTY_CLICK_LOG') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}