{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('EMAILS_AB2') }}
select
        EMAI_BODY,
        EMAI_ID,
        EMAI_NAME,
        EMAI_SUBJECT,
        EMAI_TYPE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('EMAILS_AB2') }}
-- EMAILS from {{ source('BRC', '_AIRBYTE_RAW_EMAILS') }}
where 1 = 1