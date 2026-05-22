{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('EMAIL_SENDOUT_STATS_AB2') }}
select
        EMST_FK_EMAIL,
        EMST_FK_PUBLISHER,
        EMST_ID,
        EMST_OPEN_TIME,
        EMST_SEND_STATUS,
        EMST_SEND_TIME,
        EMST_TO_EMAIL,
        EMST_UNSUB_TIME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('EMAIL_SENDOUT_STATS_AB2') }}
-- EMAIL_SENDOUT_STATS from {{ source('BRC', '_AIRBYTE_RAW_EMAIL_SENDOUT_STATS') }}
where 1 = 1