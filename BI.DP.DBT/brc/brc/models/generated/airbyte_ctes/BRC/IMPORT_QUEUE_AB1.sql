{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['impo_created'], ['impo_created']) }} as IMPO_CREATED,
        {{ json_extract_scalar('_airbyte_data', ['impo_done'], ['impo_done']) }} as IMPO_DONE,
        {{ json_extract_scalar('_airbyte_data', ['impo_ended_time'], ['impo_ended_time']) }} as IMPO_ENDED_TIME,
        {{ json_extract_scalar('_airbyte_data', ['impo_fk_adve_string'], ['impo_fk_adve_string']) }} as IMPO_FK_ADVE_STRING,
        {{ json_extract_scalar('_airbyte_data', ['impo_folder'], ['impo_folder']) }} as IMPO_FOLDER,
        {{ json_extract_scalar('_airbyte_data', ['impo_id'], ['impo_id']) }} as IMPO_ID,
        {{ json_extract_scalar('_airbyte_data', ['impo_instance'], ['impo_instance']) }} as IMPO_INSTANCE,
        {{ json_extract_scalar('_airbyte_data', ['impo_started'], ['impo_started']) }} as IMPO_STARTED,
        {{ json_extract_scalar('_airbyte_data', ['impo_started_time'], ['impo_started_time']) }} as IMPO_STARTED_TIME,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,

        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_IMPORT_QUEUE') }} as table_alias
-- IMPORT_QUEUE
where 1 = 1
