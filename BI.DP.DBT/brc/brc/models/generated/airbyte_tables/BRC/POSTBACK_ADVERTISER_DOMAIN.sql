{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('POSTBACK_ADVERTISER_DOMAIN_AB2') }}
select
	POAD_FK_ADVERTISER,
	POAD_FK_POSTBACK_DOMAIN,
	POAD_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_ADVERTISER_DOMAIN_AB2') }}
-- POSTBACK_ADVERTISER_DOMAIN from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_ADVERTISER_DOMAIN') }}
where 1 = 1