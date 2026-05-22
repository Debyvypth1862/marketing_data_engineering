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
	CONDITIONAL_PATHS_GROUPS,
	COUNTRIES,
	CREATED_TIME,
	DEFAULT_OFFER_REDIRECT_MODE,
	DEFAULT_PATHS,
	DEFAULT_PATHS_SMART_ROTATION,
	DELETED,
	ID,
	NAME,
	REALTIME_ROUTING_API,
	UPDATED_TIME,
	WORKSPACE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('FLOWS_AB2') }}