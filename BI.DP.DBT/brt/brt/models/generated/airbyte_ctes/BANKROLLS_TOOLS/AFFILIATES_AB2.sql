{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('AFFILIATES_AB1') }}
select
    cast(WHATSAPP as {{ dbt_utils.type_string() }}) as WHATSAPP,
    cast(COUNTRY as {{ dbt_utils.type_string() }}) as COUNTRY,
    cast(NOTES as {{ dbt_utils.type_string() }}) as NOTES,
    cast(CITY as {{ dbt_utils.type_string() }}) as CITY,
    cast(TIMEZONE as {{ dbt_utils.type_string() }}) as TIMEZONE,
    case
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when CREATED_AT = '' then NULL
    else to_timestamp_tz(CREATED_AT)
    end as CREATED_AT
    ,
    cast(LANGUAGE_ID as {{ dbt_utils.type_bigint() }}) as LANGUAGE_ID,
    cast(CONTACT_ID as {{ dbt_utils.type_bigint() }}) as CONTACT_ID,
    cast(ZIP_CODE as {{ dbt_utils.type_string() }}) as ZIP_CODE,
    cast(FRAUD_SCORE as {{ dbt_utils.type_bigint() }}) as FRAUD_SCORE,
    cast(SKYPE as {{ dbt_utils.type_string() }}) as SKYPE,
    cast(URL_LOGO as {{ dbt_utils.type_string() }}) as URL_LOGO,
    cast(STREET_TWO as {{ dbt_utils.type_string() }}) as STREET_TWO,
    case
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when UPDATED_AT = '' then NULL
    else to_timestamp_tz(UPDATED_AT)
    end as UPDATED_AT
    ,
    cast(MANAGER_ID as {{ dbt_utils.type_bigint() }}) as MANAGER_ID,
    cast(STREET as {{ dbt_utils.type_string() }}) as STREET,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(SIGNAL as {{ dbt_utils.type_string() }}) as SIGNAL,
    cast(SLUG as {{ dbt_utils.type_string() }}) as SLUG,
    cast(EMAIL as {{ dbt_utils.type_string() }}) as EMAIL,
    cast(TELEGRAM as {{ dbt_utils.type_string() }}) as TELEGRAM,
    cast(URL as {{ dbt_utils.type_string() }}) as URL,
    cast(DISCORD as {{ dbt_utils.type_string() }}) as DISCORD,
    cast(PHONE as {{ dbt_utils.type_string() }}) as PHONE,
    cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
    cast(REGION as {{ dbt_utils.type_string() }}) as REGION,
    cast(CURRENCY_ID as {{ dbt_utils.type_bigint() }}) as CURRENCY_ID,
    cast(STATUS as {{ dbt_utils.type_bigint() }}) as STATUS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('AFFILIATES_AB1') }}
-- AFFILIATES
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

