{{ config(
	materialized='table',
    cluster_by = ["_AIRBYTE_AB_ID", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "VOLUUM",
    tags = [ "top-level" ]
) }}
select
    AFFILIATE_NETWORK,
	ALLOWED_ACTIONS,
	CAP_CONFIGURATION,
	CONVERSION_TRACKING_METHOD,
	COUNTRY,
	CREATED_TIME,
	CURRENCY_CODE,
	DELETED,
	ID,
	MARKETPLACE,
	NAME,
	NAME_POSTFIX,
	PAYOUT,
	PREFERRED_TRACKING_DOMAIN,
	TAGS,
	UPDATED_TIME,
	URL,
	WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFERS_AB2') }}