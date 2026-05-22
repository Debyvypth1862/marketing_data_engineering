{{config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "BUFFALO_PARTNERS",
    tags = ["top-level-intermediate"]
)}}-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('REV_SHARE_REPORT_AB2') }}
select
{{ dbt_utils.surrogate_key(
        ['TRACKER_LOGIN_ID',
        'DATE',
        'AFFILIATE_ID',
        'BRAND',
        'CAMPAIGN',
        'CAMPAIGN_ID',
        'CURRENCY',
        'DATE_LAST_PLAYED',
        'DATE_OPENED',
        'DATE_FIRST_DEPOSITED',
        'DAY',
        'DEPOSITS',
        'DEVICE',
        'EARNINGS',
        'FIRST_DEPOSIT_AMOUNT',
        'GENERIC_1',
        'GENERIC_2',
        'GENERIC_3',
        'GENERIC_4',
        'GENERIC_5',
        'HIGH_ROLLER_ADJUSTED',
        'HIGH_ROLLER_ADJUSTMENT',
        'IS_NEW_ACTIVE_P',
        'IS_PLAYER_LOCKED',
        'MEDIA',
        'NET_REVENUE',
        'NUMBER_OF_DEPOSITS',
        'PLAYER_REFERENCE',
        'PRODUCT',
        ]) }} as _AIRBYTE_REV_SHARE_REPORT_HASHID,
    tmp.*
from
{{ ref('REV_SHARE_REPORT_AB2')}} tmp -- REV_SHARE_REPORT
where   1 = 1
    {{incremental_clause('_AIRBYTE_EMITTED_AT', this)}}