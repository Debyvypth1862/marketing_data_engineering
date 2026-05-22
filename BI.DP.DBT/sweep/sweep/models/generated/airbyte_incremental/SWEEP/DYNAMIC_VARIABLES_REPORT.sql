{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_NORMALIZED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "SWEEP",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('DYNAMIC_VARIABLES_REPORT_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    DATE,
    DEPOSITS,
    WITHDRAWALS,
    AFP,
    NET_DEPOSITS,
    USERID,
    COMMISSIONS,
    BRAND,
    VOLUME,
    DEPOSIT_COUNT,
    COMMISSION_COUNT,
    POSITION_COUNT,
    PL,
    TRACKING_CODE,
    TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_DYNAMIC_VARIABLES_REPORT_HASHID
from {{ ref('DYNAMIC_VARIABLES_REPORT_SCD') }}
-- DYNAMIC_VARIABLES_REPORT from {{ source('SWEEP', '_AIRBYTE_RAW_DYNAMIC_VARIABLES_REPORT_STREAM') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

