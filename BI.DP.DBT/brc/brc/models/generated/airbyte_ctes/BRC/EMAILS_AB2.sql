{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(EMAI_BODY as {{ dbt_utils.type_string() }}) as EMAI_BODY,
        try_cast(EMAI_ID as {{ dbt_utils.type_float() }}) as EMAI_ID,
        try_cast(EMAI_NAME as {{ dbt_utils.type_string() }}) as EMAI_NAME,
        try_cast(EMAI_SUBJECT as {{ dbt_utils.type_string() }}) as EMAI_SUBJECT,
        try_cast(EMAI_TYPE as {{ dbt_utils.type_string() }}) as EMAI_TYPE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('EMAILS_AB1') }}
-- EMAILS
where 1 = 1