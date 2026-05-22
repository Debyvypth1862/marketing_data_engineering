{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('REGISTRANTS_AB1') }}
select
    cast(COUNTRY as {{ dbt_utils.type_string() }}) as COUNTRY,
    cast(KEYWORDS as {{ dbt_utils.type_string() }}) as KEYWORDS,
    cast(FAVORITE_GAME as {{ dbt_utils.type_string() }}) as FAVORITE_GAME,
    cast(FORM_UUID as {{ dbt_utils.type_string() }}) as FORM_UUID,
    cast(LANGUAGE as {{ dbt_utils.type_string() }}) as LANGUAGE,
    cast(ISP_TYPE as {{ dbt_utils.type_string() }}) as ISP_TYPE,
    cast(CASINO_OFFER_PREFERENCE as {{ dbt_utils.type_string() }}) as CASINO_OFFER_PREFERENCE,
    cast(UA_JSON as {{ dbt_utils.type_string() }}) as UA_JSON,
    cast(SCORE as {{ dbt_utils.type_bigint() }}) as SCORE,
    cast(AFFILIATE_PAYMENT_STATUS as {{ dbt_utils.type_string() }}) as AFFILIATE_PAYMENT_STATUS,
    cast(PASSWORD as {{ dbt_utils.type_string() }}) as PASSWORD,
    cast(DISPOSABLE_EMAIL as {{ dbt_utils.type_string() }}) as DISPOSABLE_EMAIL,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(STATE as {{ dbt_utils.type_string() }}) as STATE,
    cast(IS_MALE as {{ dbt_utils.type_string() }}) as IS_MALE,
    cast(BROWSER_VERSION as {{ dbt_utils.type_string() }}) as BROWSER_VERSION,
    cast(MAXMIND_DATA as {{ dbt_utils.type_string() }}) as MAXMIND_DATA,
    case
        when AFFILIATE_PAYMENT_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}' then to_timestamp(AFFILIATE_PAYMENT_DATE, 'YYYY-MM-DDTHH24:MI:SS')
        when AFFILIATE_PAYMENT_DATE regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}' then to_timestamp(AFFILIATE_PAYMENT_DATE, 'YYYY-MM-DDTHH24:MI:SS.FF')
        when AFFILIATE_PAYMENT_DATE = '' then NULL
    else to_timestamp(AFFILIATE_PAYMENT_DATE)
    end as AFFILIATE_PAYMENT_DATE
    ,
    cast(CPU as {{ dbt_utils.type_string() }}) as CPU,
    cast(STATIC_IP_SCORE as {{ dbt_utils.type_string() }}) as STATIC_IP_SCORE,
    {{ cast_to_boolean('VERIFIED_EMAIL') }} as VERIFIED_EMAIL,
    cast(SCORE_FRAUD as {{ dbt_utils.type_bigint() }}) as SCORE_FRAUD,
    {{ cast_to_boolean('VERIFICATION_EMAIL_SENT') }} as VERIFICATION_EMAIL_SENT,
    cast(PHONE as {{ dbt_utils.type_string() }}) as PHONE,
    cast(USER_ID as {{ dbt_utils.type_bigint() }}) as USER_ID,
    cast(CLICK_ID as {{ dbt_utils.type_string() }}) as CLICK_ID,
    {{ cast_to_boolean('FORM_EVENTS_CLICK') }} as FORM_EVENTS_CLICK,
    cast(REGION as {{ dbt_utils.type_string() }}) as REGION,
    cast(FORM_EVENTS as {{ dbt_utils.type_string() }}) as FORM_EVENTS,
    cast(DEVICE as {{ dbt_utils.type_string() }}) as DEVICE,
    cast(PLAY_FOR as {{ dbt_utils.type_string() }}) as PLAY_FOR,
    cast(CONTINENT as {{ dbt_utils.type_string() }}) as CONTINENT,
    {{ cast_to_boolean('OPT_OUT') }} as OPT_OUT,
    cast(CITY as {{ dbt_utils.type_string() }}) as CITY,
    cast(DATE_OF_BIRTH as {{ dbt_utils.type_string() }}) as DATE_OF_BIRTH,
    cast(KICKBOX_RESPONSE as {{ dbt_utils.type_string() }}) as KICKBOX_RESPONSE,
    case
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when CREATED_AT = '' then NULL
    else to_timestamp_tz(CREATED_AT)
    end as CREATED_AT
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
    cast(BROWSER as {{ dbt_utils.type_string() }}) as BROWSER,
    cast(EMAIL_SENDEX_SCORE as {{ dbt_utils.type_float() }}) as EMAIL_SENDEX_SCORE,
    cast(OPERATING_SYSTEM as {{ dbt_utils.type_string() }}) as OPERATING_SYSTEM,
    cast(AGILECRM_ID as {{ dbt_utils.type_bigint() }}) as AGILECRM_ID,
    {{ cast_to_boolean('PUSHED_TO_AGILE_CRM') }} as PUSHED_TO_AGILE_CRM,
    cast(FIRST_NAME as {{ dbt_utils.type_string() }}) as FIRST_NAME,
    cast(EMAIL as {{ dbt_utils.type_string() }}) as EMAIL,
    cast(USER_AGENT as {{ dbt_utils.type_string() }}) as USER_AGENT,
    cast(SENDABLE_VERIFIED as {{ dbt_utils.type_string() }}) as SENDABLE_VERIFIED,
    {{ cast_to_boolean('PUSHED_TO_SNOWFLAKE') }} as PUSHED_TO_SNOWFLAKE,
    cast(LAST_NAME as {{ dbt_utils.type_string() }}) as LAST_NAME,
    cast(IP_ADDRESS as {{ dbt_utils.type_string() }}) as IP_ADDRESS,
    cast(TIME_ZONE as {{ dbt_utils.type_string() }}) as TIME_ZONE,
    cast(REGISTRATION_DOMAIN as {{ dbt_utils.type_string() }}) as REGISTRATION_DOMAIN,
    cast(URL_PARAMS as {{ dbt_utils.type_string() }}) as URL_PARAMS,
    cast(EMAIL_SENDEX as {{ dbt_utils.type_string() }}) as EMAIL_SENDEX,
    {{ cast_to_boolean('FORM_EVENTS_MOVEMENT') }} as FORM_EVENTS_MOVEMENT,
    cast(SCORE_WHALE as {{ dbt_utils.type_bigint() }}) as SCORE_WHALE,
    {{ cast_to_boolean('PUSHED_TO_FLOWS') }} as PUSHED_TO_FLOWS,
    cast(AFFILIATE_CPL as {{ dbt_utils.type_float() }}) as AFFILIATE_CPL,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('REGISTRANTS_AB1') }}
-- REGISTRANTS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

