{{ config(
	materialized='table',
    cluster_by = ["_AIRBYTE_AB_ID", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "VOLUUM",
    tags = [ "top-level" ]
) }}
select
    ALLOWED_ACTIONS,
	COUNTRY,
	CREATED_TIME,
	DELETED,
	ID,
	LANDER_TYPE,
	NAME,
	NAME_POSTFIX,
	NUMBER_OF_OFFERS,
	PREFERRED_TRACKING_DOMAIN,
	SHOULD_HAVE_TRACKING_SCRIPT,
	TAGS,
	UPDATED_TIME,
	URL,
	WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('LANDERS_AB2') }}