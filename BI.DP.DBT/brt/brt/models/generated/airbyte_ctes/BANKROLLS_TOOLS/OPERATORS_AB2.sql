{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OPERATORS_AB1') }}
select
    cast(CONTACT_SKYPE as {{ dbt_utils.type_string() }}) as CONTACT_SKYPE,
    cast(NOTES as {{ dbt_utils.type_string() }}) as NOTES,
    cast(CONTACT_PHONE as {{ dbt_utils.type_string() }}) as CONTACT_PHONE,
    cast(CITY as {{ dbt_utils.type_string() }}) as CITY,
    cast(API_URL as {{ dbt_utils.type_string() }}) as API_URL,
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
    cast(CONTACT_TELEGRAM as {{ dbt_utils.type_string() }}) as CONTACT_TELEGRAM,
    cast(CONTACT_EMAIL as {{ dbt_utils.type_string() }}) as CONTACT_EMAIL,
    cast(LOGIN_URL as {{ dbt_utils.type_string() }}) as LOGIN_URL,
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
    cast(COUNTRY_NAME as {{ dbt_utils.type_string() }}) as COUNTRY_NAME,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(SLUG as {{ dbt_utils.type_string() }}) as SLUG,
    cast(PAYMENT_METHOD as {{ dbt_utils.type_string() }}) as PAYMENT_METHOD,
    cast(CONTACT_NAME as {{ dbt_utils.type_string() }}) as CONTACT_NAME,
    cast(OPERATOR_PLATFORM_ID as {{ dbt_utils.type_string() }}) as OPERATOR_PLATFORM_ID,
    cast(CREATED_BY as {{ dbt_utils.type_bigint() }}) as CREATED_BY,
    cast(API_IP_ADDRESS as {{ dbt_utils.type_string() }}) as API_IP_ADDRESS,
    cast(LICENSE as {{ dbt_utils.type_string() }}) as LICENSE,
    cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
    cast(UPDATED_BY as {{ dbt_utils.type_bigint() }}) as UPDATED_BY,
    cast(CONTACT_WECHAT as {{ dbt_utils.type_string() }}) as CONTACT_WECHAT,
    cast(REGION as {{ dbt_utils.type_string() }}) as REGION,
    cast(CURRENCY_ID as {{ dbt_utils.type_bigint() }}) as CURRENCY_ID,
    cast(STATUS as {{ dbt_utils.type_bigint() }}) as STATUS,
    cast(ADMIN_FEE as {{ dbt_utils.type_float() }}) as ADMIN_FEE,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OPERATORS_AB1') }}
-- OPERATORS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

