{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "CELLXPERT",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('FTD_REGISTRATION_REPORT_SCD') }}
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
        PL,
        QUALIFICATION_DATE,
        REGISTRATION_DATE,
        STATUS,
        TRACKING_CODE,
        USERID,
        WITHDRAWALS,
    TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_FTD_REGISTRATION_REPORT_HASHID
from {{ ref('FTD_REGISTRATION_REPORT_SCD') }}
-- FTD_REGISTRATION_REPORT from {{ source('CELLXPERT', '_AIRBYTE_RAW_FTD_REGISTRATION_REPORT_STREAM') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

