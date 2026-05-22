{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
        try_cast(USER_ADDRESS as {{ dbt_utils.type_string() }}) as USER_ADDRESS,
        try_cast(USER_CITY as {{ dbt_utils.type_string() }}) as USER_CITY,
        try_cast(USER_COUNTRY as {{ dbt_utils.type_string() }}) as USER_COUNTRY,
        try_cast(USER_CREATED as {{ dbt_utils.type_string() }}) as USER_CREATED,
        try_cast(USER_DISPLAY_NAME as {{ dbt_utils.type_string() }}) as USER_DISPLAY_NAME,
        try_cast(USER_EMAIL as {{ dbt_utils.type_string() }}) as USER_EMAIL,
        try_cast(USER_FIRSTNAME as {{ dbt_utils.type_string() }}) as USER_FIRSTNAME,
        try_cast(USER_ID as {{ dbt_utils.type_float() }}) as USER_ID,
        try_cast(USER_IP as {{ dbt_utils.type_string() }}) as USER_IP,
        try_cast(USER_LASTNAME as {{ dbt_utils.type_string() }}) as USER_LASTNAME,
        try_cast(USER_PASSWORD as {{ dbt_utils.type_string() }}) as USER_PASSWORD,
        try_cast(USER_PHONE as {{ dbt_utils.type_string() }}) as USER_PHONE,
        try_cast(USER_REF as {{ dbt_utils.type_float() }}) as USER_REF,
        try_cast(USER_SKYPE as {{ dbt_utils.type_string() }}) as USER_SKYPE,
        try_cast(USER_STATUS as {{ dbt_utils.type_string() }}) as USER_STATUS,
        try_cast(USER_USERNAME as {{ dbt_utils.type_string() }}) as USER_USERNAME,
        try_cast(USER_ZIPCODE as {{ dbt_utils.type_string() }}) as USER_ZIPCODE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('USERS_AB1') }}
-- USERS
where 1 = 1