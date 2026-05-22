
{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
	enabled = false,
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('IMPORT_QUEUE_AB2') }}
select
        IMPO_CREATED,
        IMPO_DONE,
        IMPO_ENDED_TIME,
        IMPO_FK_ADVE_STRING,
        IMPO_FOLDER,
        IMPO_ID,
        IMPO_INSTANCE,
        IMPO_STARTED,
        IMPO_STARTED_TIME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('IMPORT_QUEUE_AB2') }}
-- IMPORT_QUEUE from {{ source('BRC', '_AIRBYTE_RAW_IMPORT_QUEUE') }}
where 1 = 1