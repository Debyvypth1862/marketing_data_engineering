{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('DAILY_STATS_TOTAL_AB2') }}
select
        DAST_CLICKS,
        DAST_CPA,
        DAST_DAY,
        DAST_DEPOSITS,
        DAST_FTD,
        DAST_ID,
        DAST_INCOME,
        DAST_MONTH,
        DAST_PAYOUT,
        DAST_PROFIT,
        DAST_SIGNUPS,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('DAILY_STATS_TOTAL_AB2') }}
-- DAILY_STATS_TOTAL from {{ source('BRC', '_AIRBYTE_RAW_DAILY_STATS_TOTAL') }}
where 1 = 1