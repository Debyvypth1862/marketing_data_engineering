{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}

select
        {{ json_extract_scalar('_airbyte_data', ['emai_body'], ['emai_body']) }} as EMAI_BODY,
        {{ json_extract_scalar('_airbyte_data', ['emai_id'], ['emai_id']) }} as EMAI_ID,
        {{ json_extract_scalar('_airbyte_data', ['emai_name'], ['emai_name']) }} as EMAI_NAME,
        {{ json_extract_scalar('_airbyte_data', ['emai_subject'], ['emai_subject']) }} as EMAI_SUBJECT,
        {{ json_extract_scalar('_airbyte_data', ['emai_type'], ['emai_type']) }} as EMAI_TYPE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('BRC', '_AIRBYTE_RAW_EMAILS') }} as table_alias
where 1 = 1
