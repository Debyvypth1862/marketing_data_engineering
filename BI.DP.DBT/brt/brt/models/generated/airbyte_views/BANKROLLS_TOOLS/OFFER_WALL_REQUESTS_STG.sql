{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('OFFER_WALL_REQUESTS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'REQUEST_X_FORWARDED_FOR',
        'NOTES',
        'OFFER_WALL_ID',
        'CLOAKER_STATUS',
        'REQUEST_URL_PARAMS',
        'CLOAKER_CONFIGURATION_ID',
        'CREATED_AT',
        'MARKETING_SITE_ID',
        'REQUEST_REFERER',
        'REQUEST_USER_AGENT',
        'UPDATED_AT',
        'REQUEST_IP_ADDRESS',
        'ID',
    ]) }} as _AIRBYTE_OFFER_WALL_REQUESTS_HASHID,
    tmp.*
from {{ ref('OFFER_WALL_REQUESTS_AB2') }} tmp
-- OFFER_WALL_REQUESTS
where 1 = 1

