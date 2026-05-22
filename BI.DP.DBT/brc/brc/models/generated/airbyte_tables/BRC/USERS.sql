{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
    database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('USERS_AB2') }}
select
        USER_ADDRESS,
        USER_CITY,
        USER_COUNTRY,
        USER_CREATED,
        USER_DISPLAY_NAME,
        USER_EMAIL,
        USER_FIRSTNAME,
        USER_ID,
        USER_IP,
        USER_LASTNAME,
        USER_PASSWORD,
        USER_PHONE,
        USER_REF,
        USER_SKYPE,
        USER_STATUS,
        USER_USERNAME,
        USER_ZIPCODE,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('USERS_AB2') }}
-- USERS from {{ source('BRC', '_AIRBYTE_RAW_USERS') }}
where 1 = 1