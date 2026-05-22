{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "CREATED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    incremental_strategy = "delete+insert",
    database = env_var('EXP_DATABASE', 'EXP'),
    schema = "PUBLIC",
    tags = [ "top-level" ]
) }}
-- Final table for API_TRAFFICX with created_at/modified_at
-- Reads from SCD table and adds user-friendly timestamp columns
-- depends_on: {{ ref('API_TRAFFICX_SCD') }}

with scd_active as (
    select
        *
    from {{ ref('API_TRAFFICX_SCD') }}
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
        min(_AIRBYTE_API_TRAFFICX_HASHID) as first_hash
    from {{ ref('API_TRAFFICX_SCD') }}
    group by _AIRBYTE_UNIQUE_KEY
)

select
    scd._AIRBYTE_UNIQUE_KEY,
    scd.DATE,
    scd.COUNTRY,
    scd.PUBLISHER,
    scd.ADVERTISER_ID,
    scd.ADVERTISER_NAME,
    scd.BRAND_NAME,
    scd.CAMPAIGN_NAME,
    scd.SUBID,
    scd.SUBID2,
    scd.SUBID3,
    scd.SUBID4,
    scd.SUBID5,
    scd."3RD_PARTY_CLICKID",
    scd.CLICKID,
    scd.CLICK_CNT,
    scd.SIGNUP_CNT,
    scd.FTD_CNT,
    scd.FTD_AMT,
    scd.CPA_CNT,
    scd.DEPOSIT_CNT,
    scd.DEPOSIT_AMT,
    scd.NET_REVENUE_AMT,
    scd.REVSHARE_REVENUE_AMT,
    scd._AIRBYTE_AB_ID,
    scd._AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    scd._AIRBYTE_API_TRAFFICX_HASHID,
    -- created_at: timestamp when record was first created (preserved from first version)
    fv.first_start_at as CREATED_AT,
    -- modified_at: NULL if data hasn't changed (hash matches first version), otherwise _AIRBYTE_START_AT
    case
        when scd._AIRBYTE_API_TRAFFICX_HASHID = fv.first_hash then null
        else scd._AIRBYTE_START_AT
    end as MODIFIED_AT
from scd_active scd
left join first_version fv
    on scd._AIRBYTE_UNIQUE_KEY = fv._AIRBYTE_UNIQUE_KEY
where (scd.CLICK_CNT > 0 and scd.SIGNUP_CNT > 0 and scd.FTD_CNT > 0)
   OR (scd.CLICK_CNT > 0 and scd.SIGNUP_CNT > 0 and scd.CPA_CNT > 0)
