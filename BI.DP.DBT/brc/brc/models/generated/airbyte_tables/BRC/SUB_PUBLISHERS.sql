{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('SUB_PUBLISHERS_AB2') }}
select
	SUBP_DATE,
	SUBP_FK_PUBLISHER,
	SUBP_FK_SUBPUBLISHER,
	SUBP_ID,
	SUBP_PERCENTAGE,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('SUB_PUBLISHERS_AB2') }}
-- SUB_PUBLISHERS from {{ source('BRC', '_AIRBYTE_RAW_SUB_PUBLISHERS') }}
where 1 = 1
