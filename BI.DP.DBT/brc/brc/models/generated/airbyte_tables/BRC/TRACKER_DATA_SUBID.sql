{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('TRACKER_DATA_SUBID_AB2') }}
select
        TDAT_CLICKS,
        TDAT_CPA,
        TDAT_DATE,
        TDAT_DEPOSIT_VALUE,
        TDAT_DEPOSITS,
        TDAT_FK_CAMPAIGN_TRACKER,
        TDAT_FK_CUSTOM,
        TDAT_NEW_DEPOSITS,
        TDAT_POSTBACK,
        TDAT_SCRIPT,
        TDAT_SIGNUPS,
        TDAT_SUBID,
        TDAT_TOTAL,
        TDAT_VIEWS,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('TRACKER_DATA_SUBID_AB2') }}
-- TRACKER_DATA_SUBID from {{ source('BRC', '_AIRBYTE_RAW_TRACKER_DATA_SUBID') }}
where 1 = 1