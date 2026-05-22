{{ config(
    cluster_by = ["_AIRBYTE_AB_ID", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "VOLUUM",
    tags = [ "top-level" ]
) }}
select
	ID,
	MEMBERSHIPS,
	NAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('WORKSPACES_AB2') }}