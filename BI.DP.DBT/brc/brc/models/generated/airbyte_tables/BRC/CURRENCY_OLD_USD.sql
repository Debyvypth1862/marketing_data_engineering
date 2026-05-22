{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CURRENCY_OLD_USD_AB2') }}
select
        CURR_DATE,
        CURR_ID,
        CURR_MONTH,
        CURR_NAME,
        CURR_VALUE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CURRENCY_OLD_USD_AB2') }}
-- CURRENCY_OLD_USD from {{ source('BRC', '_AIRBYTE_RAW_CURRENCY_OLD_USD') }}
where 1 = 1