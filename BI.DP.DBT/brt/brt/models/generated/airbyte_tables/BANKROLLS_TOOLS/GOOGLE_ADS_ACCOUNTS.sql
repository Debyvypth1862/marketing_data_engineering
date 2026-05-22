{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('GOOGLE_ADS_ACCOUNTS_AB3') }}
select
    DEVELOPER_TOKEN,
    NOTES,
    REMOTE_DESKTOP_ID,
    CREATED_AT,
    TITLE,
    CREATED_BY,
    DELETED_AT,
    URL,
    CLIENT_ID,
    ACCESS_TOKEN,
    REFRESH_TOKEN,
    PASSWORD,
    UPDATED_AT,
    PERSONA_ID,
    UPDATED_BY,
    ID,
    CLIENT_SECRET,
    EMAIL,
    STATUS,
    USERNAME,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_GOOGLE_ADS_ACCOUNTS_HASHID
from {{ ref('GOOGLE_ADS_ACCOUNTS_AB3') }}
-- GOOGLE_ADS_ACCOUNTS from {{ source('BRT', '_AIRBYTE_RAW_GOOGLE_ADS_ACCOUNTS') }}
where 1 = 1

