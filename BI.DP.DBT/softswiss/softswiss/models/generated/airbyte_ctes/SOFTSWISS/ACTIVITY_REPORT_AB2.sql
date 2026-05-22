{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "SOFTSWISS",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('ACTIVITY_REPORT_AB1') }}
select
    cast(START_DATE as {{ dbt_utils.type_string() }}) as START_DATE,
    cast(END_DATE as {{ dbt_utils.type_string() }}) as END_DATE,
    cast(DATE(DATE) as {{ dbt_utils.type_string() }}) as DATE,
    cast(BRAND_ID as {{ dbt_utils.type_string() }}) as BRAND_ID,
    cast(CAMPAIGN_ID as {{ dbt_utils.type_string() }}) as CAMPAIGN_ID,
    cast(DYNAMIC_TAG_CLICKID as {{ dbt_utils.type_string() }}) as DYNAMIC_TAG_CLICKID,
    cast(VISITS_COUNT as {{ dbt_utils.type_float() }}) as VISITS_COUNT,
    cast(REGISTRATIONS_COUNT as {{ dbt_utils.type_float() }}) as REGISTRATIONS_COUNT,
    cast(CURRENCY as {{ dbt_utils.type_string() }}) as CURRENCY,
    cast(NGR as {{ dbt_utils.type_float() }}) as NGR,
    cast(DEPOSITS_SUM as {{ dbt_utils.type_float() }}) as DEPOSITS_SUM,
    cast(DEPOSITS_COUNT as {{ dbt_utils.type_float() }}) as DEPOSIT_COUNT,
    cast(FIRST_DEPOSITS_COUNT as {{ dbt_utils.type_float() }}) as FIRST_DEPOSITS_COUNT,
    cast(FIRST_DEPOSITS_SUM as {{ dbt_utils.type_float() }}) as FIRST_DEPOSITS_SUM,
    cast(TRACKER_LOGIN_ID as {{ dbt_utils.type_float() }}) as TRACKER_LOGIN_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ACTIVITY_REPORT_AB1') }}
-- ACTIVITY_REPORT
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
