{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "INCOME_ACCESS",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('ACCOUNT_REPORT_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    DATE,
        DEPOSITS,
        COMMISSIONS,
        BONUS,
        CPA_COMMISSIONS,
        CHARGEBACKS,
        GROSS_REVENUE,
        NET_REVENUE,
        AFF_CUSTOM_ID,
        BANNER_ID,
        BANNER_TYPE,
        CPA_COMMISSION_COUNT,
        CREATIVE_NAME,
        CURRENCY_SYMBOL,
        FIRST_DEPOSIT,
        MEMBER_ID,
        MERCHANT_NAME,
        NEW,
        PLAYER_ID,
        TRACKER_LOGIN_ID,
        PLAYER_COUNTRY,
        REGISTRATION_DATE,
        ROW_ID,
        SITE_ID,
        STAKE,
        TOTAL_COMMISSION,
        TOTAL_RECORDS,
        USERNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_ACCOUNT_REPORT_HASHID
from {{ ref('ACCOUNT_REPORT_SCD') }}
-- ACCOUNT_REPORT from {{ source('INCOME_ACCESS', '_AIRBYTE_RAW_ACCOUNT_REPORT_STREAM') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

