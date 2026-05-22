{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ADMIN_LOGINS_AB2') }}
select
    LOGI_FK_ADMIN,
	LOGI_ID,
	LOGI_IP,
	LOGI_TIMESTAMP,
	LOGI_USERAGENT,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADMIN_LOGINS_AB2') }}
-- ADMIN_LOGINS from {{ source('BRC', '_AIRBYTE_RAW_ADMIN_LOGINS') }}
where 1 = 1