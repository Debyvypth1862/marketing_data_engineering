{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "EGO",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('BRAND_REPORT_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    DATE,
    AFFILIATE,
    AFFILIATE_REVENUE,
    CHARGEBACK_QTY,
    COMPLETE_DOWNLOADS,
    CREDIT_QTY,
    DYNID,
    FIRST_DEPOSITS_QTY,
    FLAT_FEE,
    FRAUD_QTY,
    HITS,
    NET_INCOME,
    REVENUE_CPA,
    REVENUE_OVERRIDE,
    REVENUE_SHARE,
    REVENUE_SUBS,
    SIGN_UPS,
    VALID_SIGN_UPS,
    VOID_QTY,
    ZONE_ID,
    REPORT,
    TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_BRAND_REPORT_HASHID
from {{ ref('BRAND_REPORT_SCD') }}
-- BRAND_REPORT from {{ source('EGO', '_AIRBYTE_RAW_BRAND_REPORT_STREAM') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

