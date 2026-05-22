{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('POSTBACK_3RD_PARTY_URLS_AB2') }}
select
	PURL_CLICK_URL,
	PURL_CPA_URL,
	PURL_FK_CAMT_ID,
	PURL_FK_PUBLISHER,
	PURL_FTD_URL,
	PURL_ID,
	PURL_SIGNUP_URL,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_3RD_PARTY_URLS_AB2') }}
-- POSTBACK_3RD_PARTY_URLS from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_3RD_PARTY_URLS') }}
where 1 = 1