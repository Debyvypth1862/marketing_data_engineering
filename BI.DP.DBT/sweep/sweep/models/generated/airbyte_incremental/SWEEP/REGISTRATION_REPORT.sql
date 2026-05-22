{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "SWEEP",
    tags = [ "top-level" ]
) }}
select
    _AIRBYTE_UNIQUE_KEY,
    DATE,
    AFP,
    COMMISSION,
    COMMISSIONS,
    COUNTRY,
    DEPOSITS,
    DEPOSIT_COUNT,
    EXTERNAL_DATE,
    FIRST_DEPOSIT,
    FIRST_DEPOSIT_DATE,
    GENERIC_1,
    GENERIC_2,
    NET_DEPOSITS,
    0 as PL,
    QUALIFICATION_DATE,
    REGISTRATION_DATE,
    STATUS,
    TRACKING_CODE,
    USERID,
    WITHDRAWALS,
    TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_REGISTRATION_REPORT_HASHID
from {{ ref('REGISTRATION_REPORT_SCD') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}