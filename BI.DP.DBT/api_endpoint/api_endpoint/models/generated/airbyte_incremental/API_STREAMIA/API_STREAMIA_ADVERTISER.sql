{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "CREATED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    database = env_var('EXP_DATABASE', 'EXP'),
    schema = "PUBLIC",
    tags = [ "top-level" ]
) }}
-- Final table for API_STREAMIA_ADVERTISER with created_at/modified_at
-- Reads from SCD table and adds user-friendly timestamp columns
-- depends_on: {{ ref('API_STREAMIA_ADVERTISER_SCD') }}

with scd_active as (
    select
        *
    from {{ ref('API_STREAMIA_ADVERTISER_SCD') }}
    where _AIRBYTE_ACTIVE_ROW = 1
    {% if is_incremental() %}
        and _AIRBYTE_EMITTED_AT > (select max(CREATED_AT) from {{ this }})
    {% endif %}
),

-- Calculate the original created_at (first version timestamp) for each unique key
first_version as (
    select
        _AIRBYTE_UNIQUE_KEY,
        min(_AIRBYTE_START_AT) as first_start_at,
        min(_AIRBYTE_API_STREAMIA_ADVERTISER_HASHID) as first_hash
    from {{ ref('API_STREAMIA_ADVERTISER_SCD') }}
    group by _AIRBYTE_UNIQUE_KEY
)

select
    scd._AIRBYTE_UNIQUE_KEY,
    scd.DATE,
    scd.TIER_LEVEL,
    scd.ADVERTISER_ID,
    scd.ADVERTISER_NAME,
    scd.AFFILIATE_ID,
    scd.CLICK_CNT,
    scd.UNIQUE_CLICKS,
    scd.SIGNUP_CNT,
    scd.FTD_CNT,
    scd.CPA_CNT,
    scd.DEPOSIT_CNT,
    scd.FTD_AMT,
    scd.DEPOSIT_AMT,
    scd.NET_DEPOSIT_AMT,
    scd.NET_REVENUE_AMT,
    scd.FTD_INCOME_AMT,
    scd.CPA_INCOME_AMT,
    scd.REVSHARE_INCOME_AMT,
    scd._AIRBYTE_AB_ID,
    scd._AIRBYTE_API_STREAMIA_ADVERTISER_HASHID,
    -- created_at: timestamp when record was first created (preserved from first version)
    fv.first_start_at as CREATED_AT,
    -- modified_at: NULL if data hasn't changed (hash matches first version), otherwise _AIRBYTE_START_AT
    case
        when scd._AIRBYTE_API_STREAMIA_ADVERTISER_HASHID = fv.first_hash then null
        else scd._AIRBYTE_START_AT
    end as MODIFIED_AT
from scd_active scd
left join first_version fv
    on scd._AIRBYTE_UNIQUE_KEY = fv._AIRBYTE_UNIQUE_KEY
