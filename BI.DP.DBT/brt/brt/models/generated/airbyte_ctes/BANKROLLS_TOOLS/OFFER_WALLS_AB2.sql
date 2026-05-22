{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OFFER_WALLS_AB1') }}
select
    cast(FEATURED as {{ dbt_utils.type_string() }}) as FEATURED,
    cast(PREVIEW_TEXT_COLOR as {{ dbt_utils.type_string() }}) as PREVIEW_TEXT_COLOR,
    {{ cast_to_boolean('LOADER') }} as LOADER,
    cast(LANGUAGE_ID as {{ dbt_utils.type_bigint() }}) as LANGUAGE_ID,
    cast(UUID as {{ dbt_utils.type_string() }}) as UUID,
    cast(RIBBON_THREE as {{ dbt_utils.type_string() }}) as RIBBON_THREE,
    {{ cast_to_boolean('REVIEWED') }} as REVIEWED,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(STATE_ID as {{ dbt_utils.type_bigint() }}) as STATE_ID,
    cast(OFFER_TYPE as {{ dbt_utils.type_string() }}) as OFFER_TYPE,
    {{ cast_to_boolean('UPDATE_APPROVED') }} as UPDATE_APPROVED,
    cast({{ adapter.quote('order') }} as {{ dbt_utils.type_string() }}) as {{ adapter.quote('order') }},
    cast(OFFERS as {{ dbt_utils.type_string() }}) as OFFERS,
    cast(RIBBON_TWO as {{ dbt_utils.type_string() }}) as RIBBON_TWO,
    {{ cast_to_boolean('SHOW_FOOTER') }} as SHOW_FOOTER,
    {{ cast_to_boolean('SHOW_PREVIEW_BUTTON') }} as SHOW_PREVIEW_BUTTON,
    cast(ORDER_TIME_PERIOD as {{ dbt_utils.type_bigint() }}) as ORDER_TIME_PERIOD,
    cast(COUNT as {{ dbt_utils.type_bigint() }}) as COUNT,
    {{ cast_to_boolean('RIBBON') }} as RIBBON,
    cast(FOOTER_SCRIPT as {{ dbt_utils.type_string() }}) as FOOTER_SCRIPT,
    cast(ORDER_DIRECTION as {{ dbt_utils.type_string() }}) as ORDER_DIRECTION,
    {{ cast_to_boolean('PARAMS') }} as PARAMS,
    {{ cast_to_boolean('IS_DEFAULT') }} as IS_DEFAULT,
    cast(VERSION as {{ dbt_utils.type_bigint() }}) as VERSION,
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
    cast(LICENSE_STATUS as {{ dbt_utils.type_string() }}) as LICENSE_STATUS,
    cast(TRAFFIC_SOURCE as {{ dbt_utils.type_string() }}) as TRAFFIC_SOURCE,
    cast(AFTER_BODY_SCRIPT as {{ dbt_utils.type_string() }}) as AFTER_BODY_SCRIPT,
    cast(USER_ID as {{ dbt_utils.type_bigint() }}) as USER_ID,
    cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
    cast(UPDATED_BY as {{ dbt_utils.type_bigint() }}) as UPDATED_BY,
    cast(RIBBON_ONE as {{ dbt_utils.type_string() }}) as RIBBON_ONE,
    cast(COUNTRY_ID as {{ dbt_utils.type_bigint() }}) as COUNTRY_ID,
    cast(LICENSED_STATES as {{ dbt_utils.type_string() }}) as LICENSED_STATES,
    cast(TEMPLATE as {{ dbt_utils.type_string() }}) as TEMPLATE,
    case
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when CREATED_AT = '' then NULL
    else to_timestamp_tz(CREATED_AT)
    end as CREATED_AT
    ,
    cast(AUTO_STARS_MINIMUM as {{ dbt_utils.type_float() }}) as AUTO_STARS_MINIMUM,
    {{ cast_to_boolean('AUTO_STARS') }} as AUTO_STARS,
    {{ cast_to_boolean('GEO') }} as GEO,
    {{ cast_to_boolean('CUSTOM_ORDER') }} as CUSTOM_ORDER,
    cast(SEARCH as {{ dbt_utils.type_string() }}) as SEARCH,
    case
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when UPDATED_AT = '' then NULL
    else to_timestamp_tz(UPDATED_AT)
    end as UPDATED_AT
    ,
    cast(PUBLISHER_KEY_ID as {{ dbt_utils.type_bigint() }}) as PUBLISHER_KEY_ID,
    case
        when ARCHIVED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(ARCHIVED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when ARCHIVED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(ARCHIVED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when ARCHIVED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(ARCHIVED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when ARCHIVED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(ARCHIVED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when ARCHIVED_AT = '' then NULL
    else to_timestamp_tz(ARCHIVED_AT)
    end as ARCHIVED_AT
    ,
    cast(PREVIEW_BUTTON_COLOR as {{ dbt_utils.type_string() }}) as PREVIEW_BUTTON_COLOR,
    cast(PAYMENT_OPTION as {{ dbt_utils.type_string() }}) as PAYMENT_OPTION,
    cast(LICENSED_COUNTRY as {{ dbt_utils.type_string() }}) as LICENSED_COUNTRY,
    {{ cast_to_boolean('BR_STATUS') }} as BR_STATUS,
    cast(ORDER_BY as {{ dbt_utils.type_string() }}) as ORDER_BY,
    cast(HEADER_SCRIPT as {{ dbt_utils.type_string() }}) as HEADER_SCRIPT,
    cast(CURRENCY_ID as {{ dbt_utils.type_bigint() }}) as CURRENCY_ID,
    cast(LANDERS as {{ dbt_utils.type_string() }}) as LANDERS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFER_WALLS_AB1') }}
-- OFFER_WALLS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

