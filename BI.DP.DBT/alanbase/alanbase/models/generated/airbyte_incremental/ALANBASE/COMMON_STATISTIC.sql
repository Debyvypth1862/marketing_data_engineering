{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "ALANBASE",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('COMMON_STATISTIC_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    Click_Count	,
    Click_Unique_Count	,
    -- hold_Count	,
    -- hold_Payout	,
    -- confirmed_Count	,
    -- confirmed_Payout	,
    -- pendingCount	,
    -- pending_Payout	,
    -- rejected_Count	,
    -- rejected_Payout,	
    total_Count as registration_count	,
    total_Count	as ftd_count,
    total_Payout as sum_ftd	,
    total_Count as deposit_count	,
    total_Payout as sum_deposit	,
    CLICKID,
    DATE,
    TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_COMMON_STATISTIC_HASHID
from {{ ref('COMMON_STATISTIC_SCD') }}
-- COMMON_STATISTIC from {{ source('ALANBASE', '_AIRBYTE_RAW_COMMON_STATISTIC') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

