{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "MYAFFILIATES",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CUSTOMER_REPORT_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
    DATE,
        PAYLOAD,
        CAMPAIGN,
        CAMPAIGN_GROUP,
        CLICKS,
        CUSTOMER,
        DEPOSITS,
        FIRST_DEPOSIT,
        FIRST_DEPOSIT_COUNT,
        IMPRESSIONS,
        INCOME,
        MEDIA,
        case when TRACKER_LOGIN_ID = 6326 then NGR
        else  NET_REVENUE end
        as NET_REVENUE,
        QUALIFIED_PLAYERS,
        SIGNUPS,
        BILLING_TITLE,
        CURRENCY_RATE,
        CURRENT_SUBSCRIPTION,
        CUSTOMER_GROUP,
        GROUP_DESCRIPTION,
        LINEAR,
        PLAN_ID,
        SUB_END_DATE,
        SUBSCRIPTION,
        SYSTEMCURRENCY,
        NDC,
        BONUSES,
        ADMIN_FEE,
        NGR,
        TOTAL_DEPOSITS,
        TOTAL_PL,
        TOTAL_STAKE,
        TOTAL_VALID_TURNOVER,
        USERCURRENCY,
        TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_CUSTOMER_REPORT_HASHID
from {{ ref('CUSTOMER_REPORT_SCD') }}
-- CUSTOMER_REPORT from {{ source('MYAFFILIATES', '_AIRBYTE_RAW_CUSTOMER_REPORT_STREAM') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

