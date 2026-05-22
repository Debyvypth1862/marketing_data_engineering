{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "CREATED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    incremental_strategy = "delete+insert",
    database = env_var('EXP_DATABASE', 'EXP'),
    schema = "BRT",
    tags = [ "top-level" ]
) }}
-- Final table for BRT_PARENT_PAST_7DAYS with created_at/modified_at
-- Reads from SCD table and adds user-friendly timestamp columns
-- depends_on: {{ ref('BRT_PARENT_PAST_7DAYS_SCD') }}

WITH scd_active AS (
    SELECT
        *
    FROM {{ ref('BRT_PARENT_PAST_7DAYS_SCD') }}
    WHERE _AIRBYTE_ACTIVE_ROW = 1
    {% if is_incremental() %}
        AND _AIRBYTE_EMITTED_AT > (SELECT MAX(CREATED_AT) FROM {{ this }})
    {% endif %}
),

-- Calculate the original created_at (first version timestamp) for each unique key
first_version AS (
    SELECT
        _AIRBYTE_UNIQUE_KEY,
        MIN(_AIRBYTE_START_AT) as first_start_at,
        MIN(_AIRBYTE_BRT_PARENT_PAST_7DAYS_HASHID) as first_hash
    FROM {{ ref('BRT_PARENT_PAST_7DAYS_SCD') }}
    GROUP BY _AIRBYTE_UNIQUE_KEY
)

SELECT
    scd._AIRBYTE_UNIQUE_KEY,
    scd.DATE_RANGE,
    scd.PARENT_ID,
    scd.BASELINE_DEPOSIT,
    scd.BASELINE_WAGER,
    scd.CPA_IN,
    scd.CPA_OUT,
    scd.CPA_DIFF,
    scd.CPL_IN,
    scd.CPL_OUT,
    scd.CPL_DIFF,
    scd.REVSHARE_IN,
    scd.REVSHARE_OUT,
    scd.REVSHARE_DIFF,
    scd.FROM_BRC,
    scd.CLICK_CNT,
    scd.FTD_CNT,
    scd.SIGNUP_CNT,
    scd.DEPOSIT_CNT,
    scd.CPA_CNT,
    scd.DEPOSIT_AMT,
    scd.NET_REVENUE_AMT,
    scd.CPA_INCOME_AMT,
    scd.CPA_PAYOUT_AMT,
    scd.CPA_REVENUE_AMT,
    scd.CPA_INCOME_PER_CLICK,
    scd.CPA_PAYOUT_PER_CLICK,
    scd.CPA_REVENUE_PER_CLICK,
    scd.CPL_INCOME_AMT,
    scd.CPL_PAYOUT_AMT,
    scd.CPL_REVENUE_AMT,
    scd.CPL_INCOME_PER_CLICK,
    scd.CPL_PAYOUT_PER_CLICK,
    scd.CPL_REVENUE_PER_CLICK,
    scd.REVSHARE_INCOME_AMT,
    scd.REVSHARE_PAYOUT_AMT,
    scd.REVSHARE_REVENUE_AMT,
    scd.REVSHARE_INCOME_PER_CLICK,
    scd.REVSHARE_PAYOUT_PER_CLICK,
    scd.REVSHARE_REVENUE_PER_CLICK,
    scd.TOTAL_INCOME_AMT,
    scd.TOTAL_PAYOUT_AMT,
    scd.TOTAL_REVENUE_AMT,
    scd.TOTAL_INCOME_PER_CLICK,
    scd.TOTAL_PAYOUT_PER_CLICK,
    scd.TOTAL_REVENUE_PER_CLICK,
    scd.CLICK_TO_SIGNUP,
    scd.CLICK_TO_FTD,
    scd.CLICK_TO_CPA,
    scd.SIGNUP_TO_FTD,
    scd.SIGNUP_TO_CPA,
    scd._AIRBYTE_AB_ID,
    scd._AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    scd._AIRBYTE_BRT_PARENT_PAST_7DAYS_HASHID,
    -- created_at: timestamp when record was first created (preserved from first version)
    fv.first_start_at as CREATED_AT,
    -- modified_at: NULL if data hasn't changed (hash matches first version), otherwise _AIRBYTE_START_AT
    CASE
        WHEN scd._AIRBYTE_BRT_PARENT_PAST_7DAYS_HASHID = fv.first_hash THEN NULL
        ELSE scd._AIRBYTE_START_AT
    END as MODIFIED_AT
FROM scd_active scd
LEFT JOIN first_version fv
    ON scd._AIRBYTE_UNIQUE_KEY = fv._AIRBYTE_UNIQUE_KEY
