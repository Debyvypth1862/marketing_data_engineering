{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('GOOGLE_ADS_ACCOUNTS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'DEVELOPER_TOKEN',
        'NOTES',
        'REMOTE_DESKTOP_ID',
        'CREATED_AT',
        'TITLE',
        'CREATED_BY',
        'DELETED_AT',
        'URL',
        'CLIENT_ID',
        'ACCESS_TOKEN',
        'REFRESH_TOKEN',
        'PASSWORD',
        'UPDATED_AT',
        'PERSONA_ID',
        'UPDATED_BY',
        'ID',
        'CLIENT_SECRET',
        'EMAIL',
        boolean_to_string('STATUS'),
        'USERNAME',
    ]) }} as _AIRBYTE_GOOGLE_ADS_ACCOUNTS_HASHID,
    tmp.*
from {{ ref('GOOGLE_ADS_ACCOUNTS_AB2') }} tmp
-- GOOGLE_ADS_ACCOUNTS
where 1 = 1

