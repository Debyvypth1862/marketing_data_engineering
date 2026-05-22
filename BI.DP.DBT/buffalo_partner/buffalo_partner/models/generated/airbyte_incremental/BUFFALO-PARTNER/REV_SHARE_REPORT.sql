{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('RAW_DATABASE'),
    schema = "BUFFALO_PARTNERS",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('REV_SHARE_REPORT_SCD') }}
select
    _AIRBYTE_UNIQUE_KEY,
   TRACKER_LOGIN_ID,
        DATE,
        AFFILIATE_ID,
        BRAND,
        CAMPAIGN,
        CAMPAIGN_ID,
        CURRENCY,
        DATE_LAST_PLAYED,
        DATE_OPENED,
        DATE_FIRST_DEPOSITED,
        DAY,
        DEPOSITS,
        DEVICE,
        EARNINGS,
        FIRST_DEPOSIT_AMOUNT,
        GENERIC_1,
        GENERIC_2,
        GENERIC_3, 
        GENERIC_4,
        GENERIC_5,
        HIGH_ROLLER_ADJUSTED,
        HIGH_ROLLER_ADJUSTMENT,
        IS_NEW_ACTIVE_P,
        IS_PLAYER_LOCKED,
        MEDIA,
        NET_REVENUE,
        NUMBER_OF_DEPOSITS,
        PLAYER_REFERENCE,
        PRODUCT,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_REV_SHARE_REPORT_HASHID
from {{ ref('REV_SHARE_REPORT_SCD') }}
-- REV_SHARE_REPORT from {{ source('BUFFALO_PARTNERS', '_AIRBYTE_RAW_REV_SHARE_REPORT_STREAM') }}
where 1 = 1
and _AIRBYTE_ACTIVE_ROW = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

