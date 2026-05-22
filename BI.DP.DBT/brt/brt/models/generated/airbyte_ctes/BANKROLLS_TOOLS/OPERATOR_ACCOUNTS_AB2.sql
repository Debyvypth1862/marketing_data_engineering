{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OPERATOR_ACCOUNTS_AB1') }}
select
    {{ cast_to_boolean('REPORT_ACTIVITY_ENABLED') }} as REPORT_ACTIVITY_ENABLED,
    cast(NOTES as {{ dbt_utils.type_string() }}) as NOTES,
    cast(CONTACT_PHONE as {{ dbt_utils.type_string() }}) as CONTACT_PHONE,
    cast(OPERATOR_ID as {{ dbt_utils.type_bigint() }}) as OPERATOR_ID,
    cast(BR_TRACKER_LOGIN_ID as {{ dbt_utils.type_bigint() }}) as BR_TRACKER_LOGIN_ID,
    cast(CPA_OUT as {{ dbt_utils.type_float() }}) as CPA_OUT,
    {{ cast_to_boolean('IS_PARENT') }} as IS_PARENT,
    cast(PASSWORD as {{ dbt_utils.type_string() }}) as PASSWORD,
    cast(URL_PIXEL as {{ dbt_utils.type_string() }}) as URL_PIXEL,
    cast(VAT_ID as {{ dbt_utils.type_string() }}) as VAT_ID,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(BR_TRACKER_LOGIN_PUBLISHER_ID as {{ dbt_utils.type_bigint() }}) as BR_TRACKER_LOGIN_PUBLISHER_ID,
    {{ cast_to_boolean('REPORT_REGISTRATION_ENABLED') }} as REPORT_REGISTRATION_ENABLED,
    cast(SCRAPER_STATUS as {{ dbt_utils.type_bigint() }}) as SCRAPER_STATUS,
    cast(URL_API as {{ dbt_utils.type_string() }}) as URL_API,
    cast(BASELINE as {{ dbt_utils.type_float() }}) as BASELINE,
    cast(CREATED_BY as {{ dbt_utils.type_bigint() }}) as CREATED_BY,
    cast(REVSHARE_IN as {{ dbt_utils.type_float() }}) as REVSHARE_IN,
    cast(PARENT_ID as {{ dbt_utils.type_bigint() }}) as PARENT_ID,
    cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
    cast(UPDATED_BY as {{ dbt_utils.type_bigint() }}) as UPDATED_BY,
    cast(CONTACT_WECHAT as {{ dbt_utils.type_string() }}) as CONTACT_WECHAT,
    {{ cast_to_boolean('REPORT_COMMISSION_ENABLED') }} as REPORT_COMMISSION_ENABLED,
    cast(STATUS as {{ dbt_utils.type_bigint() }}) as STATUS,
    cast(CONTACT_SKYPE as {{ dbt_utils.type_string() }}) as CONTACT_SKYPE,
    cast(REVSHARE_OUT as {{ dbt_utils.type_float() }}) as REVSHARE_OUT,
    cast(SECURITY_QUESTION_ANSWER as {{ dbt_utils.type_string() }}) as SECURITY_QUESTION_ANSWER,
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
    cast(INVOICING_DETAILS as {{ dbt_utils.type_string() }}) as INVOICING_DETAILS,
    cast(CONTACT_TELEGRAM as {{ dbt_utils.type_string() }}) as CONTACT_TELEGRAM,
    cast(CONTACT_EMAIL as {{ dbt_utils.type_string() }}) as CONTACT_EMAIL,
    cast(DATA_STRUCTURE as {{ dbt_utils.type_string() }}) as DATA_STRUCTURE,
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
    cast(URL_POSTBACK as {{ dbt_utils.type_string() }}) as URL_POSTBACK,
    cast(EMAIL as {{ dbt_utils.type_string() }}) as EMAIL,
    cast(CONTACT_NAME as {{ dbt_utils.type_string() }}) as CONTACT_NAME,
    {{ cast_to_boolean('API_ENABLED') }} as API_ENABLED,
    cast(URL_LOGIN as {{ dbt_utils.type_string() }}) as URL_LOGIN,
    {{ cast_to_boolean('REPORT_EARNINGS_ENABLED') }} as REPORT_EARNINGS_ENABLED,
    cast(API_STATUS as {{ dbt_utils.type_bigint() }}) as API_STATUS,
    cast(IN_HOUSE_ACCOUNT_TYPE as {{ dbt_utils.type_bigint() }}) as IN_HOUSE_ACCOUNT_TYPE,
    cast(IS_REVIEWED as {{ dbt_utils.type_bigint() }}) as IS_REVIEWED,
    cast(ACCOUNT_ID as {{ dbt_utils.type_string() }}) as ACCOUNT_ID,
    cast(API_KEY as {{ dbt_utils.type_string() }}) as API_KEY,
    cast(BR_TRACKER_LOGIN_ADVERTISER_ID as {{ dbt_utils.type_bigint() }}) as BR_TRACKER_LOGIN_ADVERTISER_ID,
    {{ cast_to_boolean('KEEPER_IMPORTED') }} as KEEPER_IMPORTED,
    cast(CPA_IN as {{ dbt_utils.type_float() }}) as CPA_IN,
    cast(ADMIN_FEE as {{ dbt_utils.type_float() }}) as ADMIN_FEE,
    cast(IS_APPROVED as {{ dbt_utils.type_bigint() }}) as IS_APPROVED,
    cast(USERNAME as {{ dbt_utils.type_string() }}) as USERNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OPERATOR_ACCOUNTS_AB1') }}
-- OPERATOR_ACCOUNTS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

