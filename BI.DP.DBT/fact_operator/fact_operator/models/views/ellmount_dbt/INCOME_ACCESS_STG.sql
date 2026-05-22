{{ config(
    database = env_var('STG_DATABASE'),
    schema = "PUBLIC",
    tags = ["top-level-intermediate"]
) }}-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('INCOME_ACCESS_AB') }}
SELECT
{{ dbt_utils.surrogate_key(
        ['DATE',
            'SIGNUP_DATE',
            'FTD_DATE',
            'FTD_DATE_AGG',
            'COUNTRY',
            'PUBLISHER_NAME',
            'ADVERTISER_ID',
            'ADVERTISER_NAME',
            'BRAND_NAME',
            'PLAYER_IPADDRESS',
            'CLICKID',
            'CLICK_CNT',
            'SIGNUP_CNT',
            'FTD_CNT',
            'FTD_AMT',
            'WITHDRAWAL_AMT',
            'COMMISSION_AMT',
            'DEPOSIT_CNT',
            'DEPOSIT_AMT',
            'NET_DEPOSIT_AMT',
            'NET_REVENUE_AMT',
            'TRACKER_LOGIN_ID',
            'TRACKER_USERNAME',
            'OPERATOR_PLATFORM'
        ]) }} AS _AIRBYTE_ACTIVITY_REPORT_HASHID
  , tmp.*
FROM
  {{ ref('INCOME_ACCESS_AB') }} AS tmp
