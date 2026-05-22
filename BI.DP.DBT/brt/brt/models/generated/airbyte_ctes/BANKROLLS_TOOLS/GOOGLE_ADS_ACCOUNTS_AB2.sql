{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('GOOGLE_ADS_ACCOUNTS_AB1') }}
select
    cast(DEVELOPER_TOKEN as {{ dbt_utils.type_string() }}) as DEVELOPER_TOKEN,
    cast(NOTES as {{ dbt_utils.type_string() }}) as NOTES,
    cast(REMOTE_DESKTOP_ID as {{ dbt_utils.type_bigint() }}) as REMOTE_DESKTOP_ID,
    case
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when CREATED_AT = '' then NULL
    else to_timestamp_tz(CREATED_AT)
    end as CREATED_AT
    ,
    cast(TITLE as {{ dbt_utils.type_string() }}) as TITLE,
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
    cast(URL as {{ dbt_utils.type_string() }}) as URL,
    cast(CLIENT_ID as {{ dbt_utils.type_string() }}) as CLIENT_ID,
    cast(ACCESS_TOKEN as {{ dbt_utils.type_string() }}) as ACCESS_TOKEN,
    cast(REFRESH_TOKEN as {{ dbt_utils.type_string() }}) as REFRESH_TOKEN,
    cast(PASSWORD as {{ dbt_utils.type_string() }}) as PASSWORD,
    case
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when UPDATED_AT = '' then NULL
    else to_timestamp_tz(UPDATED_AT)
    end as UPDATED_AT
    ,
    cast(PERSONA_ID as {{ dbt_utils.type_bigint() }}) as PERSONA_ID,
    cast(UPDATED_BY as {{ dbt_utils.type_bigint() }}) as UPDATED_BY,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(CLIENT_SECRET as {{ dbt_utils.type_string() }}) as CLIENT_SECRET,
    cast(EMAIL as {{ dbt_utils.type_string() }}) as EMAIL,
    {{ cast_to_boolean('STATUS') }} as STATUS,
    cast(USERNAME as {{ dbt_utils.type_string() }}) as USERNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('GOOGLE_ADS_ACCOUNTS_AB1') }}
-- GOOGLE_ADS_ACCOUNTS
where 1 = 1

