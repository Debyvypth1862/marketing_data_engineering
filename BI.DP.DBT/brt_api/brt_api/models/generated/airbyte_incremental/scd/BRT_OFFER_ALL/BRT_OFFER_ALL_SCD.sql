{{ config(
    cluster_by = ["_AIRBYTE_ACTIVE_ROW", "_AIRBYTE_UNIQUE_KEY_SCD", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY_SCD",
    database = env_var('INTM_DATABASE', 'INTM'),
    schema = "BRT",
    full_refresh = false,
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=env_var('EXP_DATABASE', 'EXP'),
                            schema='PUBLIC',
                            identifier='BRT_OFFER_ALL'
                        )
                    %}
                    {%
                    if final_table_relation is not none and '_AIRBYTE_UNIQUE_KEY' in adapter.get_columns_in_relation(final_table_relation)|map(attribute='name')
                    %}
                    -- Delete records which are no longer active
                    delete from {{ final_table_relation }} where {{ final_table_relation }}._AIRBYTE_UNIQUE_KEY in (
                        select recent_records.unique_key
                        from (
                                select distinct _AIRBYTE_UNIQUE_KEY as unique_key
                                from {{ this }}
                                where 1=1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', env_var('EXP_DATABASE', 'EXP') ~ '.PUBLIC.BRT_OFFER_ALL') }}
                            ) recent_records
                            left join (
                                select _AIRBYTE_UNIQUE_KEY as unique_key, count(_AIRBYTE_UNIQUE_KEY) as active_count
                                from {{ this }}
                                where _AIRBYTE_ACTIVE_ROW = 1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', env_var('EXP_DATABASE', 'EXP') ~ '.PUBLIC.BRT_OFFER_ALL') }}
                                group by _AIRBYTE_UNIQUE_KEY
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ",
                    "drop view {{ env_var('INTM_DATABASE', 'INTM') }}.BRT.BRT_OFFER_ALL_STG"],
    tags = [ "top-level" ]
) }}
-- SCD Type 2 model for BRT_OFFER_ALL
-- depends_on: {{ ref('BRT_OFFER_ALL_STG') }}
WITH
{% if is_incremental() %}
new_data AS (
    -- retrieve incremental "new" data
    SELECT
        *
    FROM {{ ref('BRT_OFFER_ALL_STG') }}
    WHERE 1 = 1
    {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
),
new_data_ids AS (
    -- build a subset of _AIRBYTE_UNIQUE_KEY from rows that are new
    SELECT DISTINCT
        {{ dbt_utils.surrogate_key([
            'CLICK_DATE',
            'OFFER_ID',
        ]) }} as _AIRBYTE_UNIQUE_KEY
    FROM new_data
),
empty_new_data AS (
    -- build an empty table to only keep the table's column types
    SELECT * FROM new_data WHERE 1 = 0
),
previous_active_scd_data AS (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    SELECT
        {{ star_intersect(ref('BRT_OFFER_ALL_STG'), this, from_alias='inc_data', intersect_alias='this_data') }}
    FROM {{ this }} AS this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    JOIN new_data_ids ON this_data._AIRBYTE_UNIQUE_KEY = new_data_ids._AIRBYTE_UNIQUE_KEY
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    LEFT JOIN empty_new_data AS inc_data ON this_data._AIRBYTE_AB_ID = inc_data._AIRBYTE_AB_ID
    WHERE _AIRBYTE_ACTIVE_ROW = 1
),
input_data AS (
    SELECT {{ dbt_utils.star(ref('BRT_OFFER_ALL_STG')) }} FROM new_data
    UNION ALL
    SELECT {{ dbt_utils.star(ref('BRT_OFFER_ALL_STG')) }} FROM previous_active_scd_data
),
{% else %}
input_data AS (
    SELECT *
    FROM {{ ref('BRT_OFFER_ALL_STG') }}
),
{% endif %}
scd_data AS (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    SELECT
        {{ dbt_utils.surrogate_key([
            'CLICK_DATE',
            'OFFER_ID',
        ]) }} as _AIRBYTE_UNIQUE_KEY,
        CLICK_DATE,
        OFFER_ID,
        BASELINE_DEPOSIT,
        BASELINE_WAGER,
        CPA_IN,
        CPA_OUT,
        CPA_DIFF,
        CPL_IN,
        CPL_OUT,
        CPL_DIFF,
        REVSHARE_IN,
        REVSHARE_OUT,
        REVSHARE_DIFF,
        FROM_BRC,
        CLICK_CNT,
        FTD_CNT,
        SIGNUP_CNT,
        DEPOSIT_CNT,
        CPA_CNT,
        DEPOSIT_AMT,
        NET_REVENUE_AMT,
        CPA_INCOME_AMT,
        CPA_PAYOUT_AMT,
        CPA_REVENUE_AMT,
        CPA_INCOME_PER_CLICK,
        CPA_PAYOUT_PER_CLICK,
        CPA_REVENUE_PER_CLICK,
        CPL_INCOME_AMT,
        CPL_PAYOUT_AMT,
        CPL_REVENUE_AMT,
        CPL_INCOME_PER_CLICK,
        CPL_PAYOUT_PER_CLICK,
        CPL_REVENUE_PER_CLICK,
        REVSHARE_INCOME_AMT,
        REVSHARE_PAYOUT_AMT,
        REVSHARE_REVENUE_AMT,
        REVSHARE_INCOME_PER_CLICK,
        REVSHARE_PAYOUT_PER_CLICK,
        REVSHARE_REVENUE_PER_CLICK,
        TOTAL_INCOME_AMT,
        TOTAL_PAYOUT_AMT,
        TOTAL_REVENUE_AMT,
        TOTAL_INCOME_PER_CLICK,
        TOTAL_PAYOUT_PER_CLICK,
        TOTAL_REVENUE_PER_CLICK,
        CLICK_TO_SIGNUP,
        CLICK_TO_FTD,
        CLICK_TO_CPA,
        SIGNUP_TO_FTD,
        SIGNUP_TO_CPA,
        _AIRBYTE_EMITTED_AT as _AIRBYTE_START_AT,
        LAG(_AIRBYTE_EMITTED_AT) OVER (
            PARTITION BY CLICK_DATE, OFFER_ID
            ORDER BY
                CLICK_DATE IS NULL ASC,
                CLICK_DATE DESC,
                _AIRBYTE_EMITTED_AT DESC
        ) as _AIRBYTE_END_AT,
        CASE WHEN ROW_NUMBER() OVER (
            PARTITION BY CLICK_DATE, OFFER_ID
            ORDER BY
                CLICK_DATE IS NULL ASC,
                CLICK_DATE DESC,
                _AIRBYTE_EMITTED_AT DESC
        ) = 1 THEN 1 ELSE 0 END as _AIRBYTE_ACTIVE_ROW,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        _AIRBYTE_BRT_OFFER_ALL_HASHID
    FROM input_data
),
dedup_data AS (
    SELECT
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        ROW_NUMBER() OVER (
            PARTITION BY
                _AIRBYTE_UNIQUE_KEY,
                _AIRBYTE_START_AT,
                _AIRBYTE_EMITTED_AT
            ORDER BY _AIRBYTE_ACTIVE_ROW DESC, _AIRBYTE_AB_ID
        ) as _AIRBYTE_ROW_NUM,
        {{ dbt_utils.surrogate_key([
            '_AIRBYTE_UNIQUE_KEY',
            '_AIRBYTE_START_AT',
            '_AIRBYTE_EMITTED_AT'
        ]) }} as _AIRBYTE_UNIQUE_KEY_SCD,
        scd_data.*
    FROM scd_data
)
SELECT
    _AIRBYTE_UNIQUE_KEY,
    _AIRBYTE_UNIQUE_KEY_SCD,
    CLICK_DATE,
    OFFER_ID,
    BASELINE_DEPOSIT,
    BASELINE_WAGER,
    CPA_IN,
    CPA_OUT,
    CPA_DIFF,
    CPL_IN,
    CPL_OUT,
    CPL_DIFF,
    REVSHARE_IN,
    REVSHARE_OUT,
    REVSHARE_DIFF,
    FROM_BRC,
    CLICK_CNT,
    FTD_CNT,
    SIGNUP_CNT,
    DEPOSIT_CNT,
    CPA_CNT,
    DEPOSIT_AMT,
    NET_REVENUE_AMT,
    CPA_INCOME_AMT,
    CPA_PAYOUT_AMT,
    CPA_REVENUE_AMT,
    CPA_INCOME_PER_CLICK,
    CPA_PAYOUT_PER_CLICK,
    CPA_REVENUE_PER_CLICK,
    CPL_INCOME_AMT,
    CPL_PAYOUT_AMT,
    CPL_REVENUE_AMT,
    CPL_INCOME_PER_CLICK,
    CPL_PAYOUT_PER_CLICK,
    CPL_REVENUE_PER_CLICK,
    REVSHARE_INCOME_AMT,
    REVSHARE_PAYOUT_AMT,
    REVSHARE_REVENUE_AMT,
    REVSHARE_INCOME_PER_CLICK,
    REVSHARE_PAYOUT_PER_CLICK,
    REVSHARE_REVENUE_PER_CLICK,
    TOTAL_INCOME_AMT,
    TOTAL_PAYOUT_AMT,
    TOTAL_REVENUE_AMT,
    TOTAL_INCOME_PER_CLICK,
    TOTAL_PAYOUT_PER_CLICK,
    TOTAL_REVENUE_PER_CLICK,
    CLICK_TO_SIGNUP,
    CLICK_TO_FTD,
    CLICK_TO_CPA,
    SIGNUP_TO_FTD,
    SIGNUP_TO_CPA,
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_BRT_OFFER_ALL_HASHID
FROM dedup_data WHERE _AIRBYTE_ROW_NUM = 1
