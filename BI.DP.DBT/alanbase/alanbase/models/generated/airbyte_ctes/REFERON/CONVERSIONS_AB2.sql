{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "REFERON",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('CONVERSIONS_AB1') }}
select
    cast(conversion_id as {{ dbt_utils.type_string() }}) as conversion_id,
    cast(status as {{ dbt_utils.type_string() }}) as status,
    cast(conversion_datetime as {{ dbt_utils.type_string() }}) as conversion_datetime,
    cast(payment_model as {{ dbt_utils.type_string() }}) as payment_model,
    cast(payout as {{ dbt_utils.type_string() }}) as payout,
    cast(payout_currency as {{ dbt_utils.type_string() }}) as payout_currency,
    cast(sub1 as {{ dbt_utils.type_string() }}) as sub1,
    cast(sub2 as {{ dbt_utils.type_string() }}) as sub2,
    cast(edited_by_manager as {{ dbt_utils.type_string() }}) as edited_by_manager,
    cast(click_id as {{ dbt_utils.type_string() }}) as click_id,
    cast(click_datetime as {{ dbt_utils.type_string() }}) as click_datetime,
    cast(click_redirect_url as {{ dbt_utils.type_string() }}) as click_redirect_url,
    cast(click_ip as {{ dbt_utils.type_string() }}) as click_ip,
    cast(browser as {{ dbt_utils.type_string() }}) as browser,
    cast(os as {{ dbt_utils.type_string() }}) as os,
    cast(device_type as {{ dbt_utils.type_string() }}) as device_type,
    cast(country as {{ dbt_utils.type_string() }}) as country,
    cast(referer as {{ dbt_utils.type_string() }}) as referer,
    cast(condition_id as {{ dbt_utils.type_string() }}) as condition_id,
    cast(is_qualification as {{ dbt_utils.type_string() }}) as is_qualification,
    cast(user_agent as {{ dbt_utils.type_string() }}) as user_agent,
    cast(landing_id as {{ dbt_utils.type_string() }}) as landing_id,
    cast(goal as {{ dbt_utils.type_string() }}) as goal,
    cast(offer_id as {{ dbt_utils.type_string() }}) as offer_id,
    cast(offer_name as {{ dbt_utils.type_string() }}) as offer_name,
    cast(offer_tags as {{ dbt_utils.type_string() }}) as offer_tags,
    cast(partner_id as {{ dbt_utils.type_string() }}) as partner_id,
    cast(partner_email as {{ dbt_utils.type_string() }}) as partner_email,
    cast(date as {{ dbt_utils.type_string() }}) as date,
    cast(tracker_login_id as {{ dbt_utils.type_string() }}) as tracker_login_id,
    cast(decline_reason as {{ dbt_utils.type_string() }}) as decline_reason,
    _AIRBYTE_AB_ID,
    cast(_AIRBYTE_EMITTED_AT as TIMESTAMP_TZ(9)) as _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('CONVERSIONS_AB1') }}
-- CONVERSIONS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
-- AND NET_REVENUE IS NOT NULL
-- AND CLICK_ID IS NOT NULL
