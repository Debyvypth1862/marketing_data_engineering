{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OFFER_WALL_SECTIONS_AB1') }}
select
    cast(FOOTER_SCRIPT as {{ dbt_utils.type_string() }}) as FOOTER_SCRIPT,
    cast(CLOAKER_CONFIGURATION_ID as {{ dbt_utils.type_bigint() }}) as CLOAKER_CONFIGURATION_ID,
    case
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when CREATED_AT = '' then NULL
    else to_timestamp_tz(CREATED_AT)
    end as CREATED_AT
    ,
    cast(MARKETING_SITE_ID as {{ dbt_utils.type_bigint() }}) as MARKETING_SITE_ID,
    {{ cast_to_boolean('PARAMS') }} as PARAMS,
    cast(UUID as {{ dbt_utils.type_string() }}) as UUID,
    cast(CREATED_BY as {{ dbt_utils.type_bigint() }}) as CREATED_BY,
    case
        when DELETED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(DELETED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when DELETED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(DELETED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when DELETED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(DELETED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when DELETED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(DELETED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when DELETED_AT = '' then NULL
    else to_timestamp_tz(DELETED_AT)
    end as DELETED_AT
    ,
    case
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when UPDATED_AT = '' then NULL
    else to_timestamp_tz(UPDATED_AT)
    end as UPDATED_AT
    ,
    cast(AFTER_BODY_SCRIPT as {{ dbt_utils.type_string() }}) as AFTER_BODY_SCRIPT,
    cast(USER_ID as {{ dbt_utils.type_bigint() }}) as USER_ID,
    cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
    cast(UPDATED_BY as {{ dbt_utils.type_bigint() }}) as UPDATED_BY,
    cast(HEADER_SCRIPT as {{ dbt_utils.type_string() }}) as HEADER_SCRIPT,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFER_WALL_SECTIONS_AB1') }}
-- OFFER_WALL_SECTIONS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

