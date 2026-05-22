{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('TRACKER_LOGINS_AB2') }}
select
        TLOG_ACCOUNT_ID,
        TLOG_CREATED,
        TLOG_CREATED_BY,
        TLOG_DELETED,
        TLOG_DONT_IMPORT,
        TLOG_ERROR,
        TLOG_FK_ADVERTISER,
        TLOG_FK_PUBLISHER,
        TLOG_HISTORY,
        TLOG_ID,
        TLOG_IMPORT_TIME,
        TLOG_NOTE,
        TLOG_PASSWORD,
        TLOG_REMOTE_KEY1,
        TLOG_REMOTE_KEY2,
        TLOG_REMOTE_KEY3,
        TLOG_STATUS,
        TLOG_STATUS_DATE,
        TLOG_SUB,
        TLOG_TYPE,
        TLOG_UPDATED,
        TLOG_UPDATED_BY,
        TLOG_USERNAME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('TRACKER_LOGINS_AB2') }}
-- TRACKER_LOGINS from {{ source('BRC', '_AIRBYTE_RAW_TRACKER_LOGINS') }}
where 1 = 1