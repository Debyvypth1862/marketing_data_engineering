{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OFFERS_HISTORY_AB1') }}
select
    cast({{ empty_string_to_null('DATE') }} as {{ type_date() }}) as DATE,
    cast(REVSHARE_OPERATOR as {{ dbt_utils.type_float() }}) as REVSHARE_OPERATOR,
    cast(CPL_OPERATOR as {{ dbt_utils.type_float() }}) as CPL_OPERATOR,
    cast(REVSHARE_DIFF as {{ dbt_utils.type_float() }}) as REVSHARE_DIFF,
    cast(CR_SIGNUP_TO_FTD_UNVERIFIED as {{ dbt_utils.type_float() }}) as CR_SIGNUP_TO_FTD_UNVERIFIED,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(CPA_AFFILIATE as {{ dbt_utils.type_float() }}) as CPA_AFFILIATE,
    cast(CPA_REVENUE_PER_CLICK as {{ dbt_utils.type_float() }}) as CPA_REVENUE_PER_CLICK,
    cast(CR_SIGNUP_TO_FTD_VERIFIED as {{ dbt_utils.type_float() }}) as CR_SIGNUP_TO_FTD_VERIFIED,
    cast(SIGN_UPS as {{ dbt_utils.type_float() }}) as SIGN_UPS,
    cast(CPL_DIFF as {{ dbt_utils.type_float() }}) as CPL_DIFF,
    cast(TOTAL_INCOME_PER_CLICK as {{ dbt_utils.type_float() }}) as TOTAL_INCOME_PER_CLICK,
    cast(BASELINE as {{ dbt_utils.type_float() }}) as BASELINE,
    cast(DEPOSITS as {{ dbt_utils.type_float() }}) as DEPOSITS,
    cast(TOTAL_REVENUE_PER_CLICK as {{ dbt_utils.type_float() }}) as TOTAL_REVENUE_PER_CLICK,
    cast(CPL_PAYOUT as {{ dbt_utils.type_float() }}) as CPL_PAYOUT,
    cast(TOTAL_INCOME as {{ dbt_utils.type_float() }}) as TOTAL_INCOME,
    cast(CPL_REVENUE_PER_CLICK as {{ dbt_utils.type_float() }}) as CPL_REVENUE_PER_CLICK,
    cast(CPA_INCOME as {{ dbt_utils.type_float() }}) as CPA_INCOME,
    cast(TOTAL_PAYOUT_UNVERIFIED as {{ dbt_utils.type_float() }}) as TOTAL_PAYOUT_UNVERIFIED,
    cast(REVSHARE_REVENUE as {{ dbt_utils.type_float() }}) as REVSHARE_REVENUE,
    cast(REVSHARE_PAYOUT as {{ dbt_utils.type_float() }}) as REVSHARE_PAYOUT,
    cast(REVSHARE_AFFILIATE as {{ dbt_utils.type_float() }}) as REVSHARE_AFFILIATE,
    cast(CR_CLICK_TO_SIGNUP as {{ dbt_utils.type_float() }}) as CR_CLICK_TO_SIGNUP,
    cast(CR_CLICK_TO_FTD_UNVERIFIED as {{ dbt_utils.type_float() }}) as CR_CLICK_TO_FTD_UNVERIFIED,
    cast(REVSHARE_INCOME_PER_CLICK as {{ dbt_utils.type_float() }}) as REVSHARE_INCOME_PER_CLICK,
    cast(REVSHARE_REVENUE_PER_CLICK as {{ dbt_utils.type_float() }}) as REVSHARE_REVENUE_PER_CLICK,
    case
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when CREATED_AT = '' then NULL
    else to_timestamp_tz(CREATED_AT)
    end as CREATED_AT
    ,
    cast(CPA_OPERATOR as {{ dbt_utils.type_float() }}) as CPA_OPERATOR,
    cast(CR_CLICK_TO_FTD_VERIFIED as {{ dbt_utils.type_float() }}) as CR_CLICK_TO_FTD_VERIFIED,
    case
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when UPDATED_AT = '' then NULL
    else to_timestamp_tz(UPDATED_AT)
    end as UPDATED_AT
    ,
    cast(CPA_INCOME_PER_CLICK as {{ dbt_utils.type_float() }}) as CPA_INCOME_PER_CLICK,
    cast(TOTAL_PAYOUT_PER_CLICK_UNVERIFIED as {{ dbt_utils.type_float() }}) as TOTAL_PAYOUT_PER_CLICK_UNVERIFIED,
    cast(TOTAL_PAYOUT_VERIFIED as {{ dbt_utils.type_float() }}) as TOTAL_PAYOUT_VERIFIED,
    cast(FTDS_UNVERIFIED as {{ dbt_utils.type_float() }}) as FTDS_UNVERIFIED,
    cast(CPL_PAYOUT_PER_CLICK as {{ dbt_utils.type_float() }}) as CPL_PAYOUT_PER_CLICK,
    cast(REVSHARE_INCOME as {{ dbt_utils.type_float() }}) as REVSHARE_INCOME,
    cast(CPL_INCOME_PER_CLICK as {{ dbt_utils.type_float() }}) as CPL_INCOME_PER_CLICK,
    cast(CPL_INCOME as {{ dbt_utils.type_float() }}) as CPL_INCOME,
    cast(FTDS_VERIFIED as {{ dbt_utils.type_float() }}) as FTDS_VERIFIED,
    cast(REVSHARE_PAYOUT_PER_CLICK as {{ dbt_utils.type_float() }}) as REVSHARE_PAYOUT_PER_CLICK,
    cast(CPA_DIFF as {{ dbt_utils.type_float() }}) as CPA_DIFF,
    cast(OFFER_ID as {{ dbt_utils.type_bigint() }}) as OFFER_ID,
    cast(CPA_REVENUE as {{ dbt_utils.type_float() }}) as CPA_REVENUE,
    cast(CPL_REVENUE as {{ dbt_utils.type_float() }}) as CPL_REVENUE,
    cast(CPA_PAYOUT_PER_CLICK as {{ dbt_utils.type_float() }}) as CPA_PAYOUT_PER_CLICK,
    cast(CPA_PAYOUT as {{ dbt_utils.type_float() }}) as CPA_PAYOUT,
    cast(TOTAL_REVENUE as {{ dbt_utils.type_float() }}) as TOTAL_REVENUE,
    cast(CLICKS as {{ dbt_utils.type_float() }}) as CLICKS,
    cast(CPL_AFFILIATE as {{ dbt_utils.type_float() }}) as CPL_AFFILIATE,
    cast(TOTAL_PAYOUT_PER_CLICK_VERIFIED as {{ dbt_utils.type_float() }}) as TOTAL_PAYOUT_PER_CLICK_VERIFIED,
    S3_PATH,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFERS_HISTORY_AB1') }}
-- OFFERS_HISTORY
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

