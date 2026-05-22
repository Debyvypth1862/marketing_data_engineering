{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('IMPORT_STATUS_AB2') }}
select
        IMST_ID,
        IMST_NAME,
        IMST_OUTPUT,
        IMST_TIMESTAMP,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('IMPORT_STATUS_AB2') }}
-- IMPORT_STATUS from {{ source('BRC', '_AIRBYTE_RAW_IMPORT_STATUS') }}
where 1 = 1