{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "Q_PLATFORM",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('UTM_CODE_REPORT_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    START_DATE,
    END_DATE,
    MERCHANT,
    AFFILIATE_ID,
    AN_ID,
    ANID1,
    ANID2,
    ANID3,
    ANID4,
    ANID5,
    CPA_PROFIT,
    CPL_PROFIT,
    CREATIVE_ID,
    DEPOSITS,
    GGR,
    MERCHANT_NAME,
    NGR,
    PROFIT,
    REVENUE_SHARE_PROFIT,
    SERIAL_ID,
    SITE_ID,
    TRANSACTION_DATE,
    WITHDRAWALS,
    TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_UTM_CODE_REPORT_HASHID
from {{ ref('UTM_CODE_REPORT_SCD') }}
-- UTM_CODE_REPORT from {{ source('Q_PLATFORM', '_AIRBYTE_RAW_UTM_CODE_REPORT_STREAM') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

