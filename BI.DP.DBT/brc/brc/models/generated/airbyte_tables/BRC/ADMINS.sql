{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ADMINS_AB2') }}
select
    ADMI_COLOR,
	ADMI_CREATED,
	ADMI_DELETED,
	ADMI_DISPLAY_NAME,
	ADMI_EMAIL,
	ADMI_ID,
	ADMI_IP,
	ADMI_LAST_LOGIN,
	ADMI_LEVEL,
	ADMI_PASSWORD,
	ADMI_PUBLISHER_MANAGER,
	ADMI_SKYPE,
	ADMI_TELEGRAM,
	ADMI_USERNAME,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADMINS_AB2') }}
-- ADMINS from {{ source('BRC', '_AIRBYTE_RAW_ADMINS') }}
where 1 = 1