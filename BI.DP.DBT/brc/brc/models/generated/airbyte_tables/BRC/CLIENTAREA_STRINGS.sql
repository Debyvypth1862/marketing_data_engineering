{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('CLIENTAREA_STRINGS_AB2') }}
select
        STRING_EN,
        STRING_ID,
        STRING_NAME,
        STRING_PAGE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CLIENTAREA_STRINGS_AB2') }}
-- CLIENTAREA_STRINGS from {{ source('BRC', '_AIRBYTE_RAW_CLIENTAREA_STRINGS') }}
where 1 = 1