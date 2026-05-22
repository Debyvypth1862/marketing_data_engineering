{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('OFFERS_AB1') }}
select
    {{ cast_to_boolean('NGR_INSUFFICIENT_DATA') }} as NGR_INSUFFICIENT_DATA,
    cast(CPL_OPERATOR as {{ dbt_utils.type_float() }}) as CPL_OPERATOR,
    cast(REVSHARE_DIFF as {{ dbt_utils.type_float() }}) as REVSHARE_DIFF,
    cast(BONUS_OFFER_LINE_3 as {{ dbt_utils.type_string() }}) as BONUS_OFFER_LINE_3,
    cast(BONUS_OFFER_LINE_6 as {{ dbt_utils.type_string() }}) as BONUS_OFFER_LINE_6,
    cast(REVIEW as {{ dbt_utils.type_string() }}) as REVIEW,
    cast(BONUS_OFFER_LINE_5 as {{ dbt_utils.type_string() }}) as BONUS_OFFER_LINE_5,
    cast(BONUS_OFFER_LINE_4 as {{ dbt_utils.type_string() }}) as BONUS_OFFER_LINE_4,
    cast(BULLET_THREE as {{ dbt_utils.type_string() }}) as BULLET_THREE,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(PAST_NGR_PER_PLAYER_1M as {{ dbt_utils.type_float() }}) as PAST_NGR_PER_PLAYER_1M,
    case
        when NGR_PROCESSED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(NGR_PROCESSED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when NGR_PROCESSED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(NGR_PROCESSED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when NGR_PROCESSED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(NGR_PROCESSED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when NGR_PROCESSED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(NGR_PROCESSED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when NGR_PROCESSED_AT = '' then NULL
    else to_timestamp_tz(NGR_PROCESSED_AT)
    end as NGR_PROCESSED_AT
    ,
    cast(CPA_REVENUE_PER_CLICK as {{ dbt_utils.type_float() }}) as CPA_REVENUE_PER_CLICK,
    {{ cast_to_boolean('UPDATE_APPROVED') }} as UPDATE_APPROVED,
    cast(REVIEW_COUNT_TITLE as {{ dbt_utils.type_string() }}) as REVIEW_COUNT_TITLE,
    cast(SIGN_UPS as {{ dbt_utils.type_bigint() }}) as SIGN_UPS,
    cast(CPL_DIFF as {{ dbt_utils.type_float() }}) as CPL_DIFF,
    cast(TOTAL_INCOME_PER_CLICK as {{ dbt_utils.type_float() }}) as TOTAL_INCOME_PER_CLICK,
    cast(PREDICTED_NGR_PER_PLAYER_6M as {{ dbt_utils.type_float() }}) as PREDICTED_NGR_PER_PLAYER_6M,
    {{ cast_to_boolean('ACTIVE') }} as ACTIVE,
    cast(BASELINE as {{ dbt_utils.type_bigint() }}) as BASELINE,
    cast(LICENSE_STATUS as {{ dbt_utils.type_string() }}) as LICENSE_STATUS,
    cast(TRAFFIC_SOURCE as {{ dbt_utils.type_string() }}) as TRAFFIC_SOURCE,
    {{ cast_to_boolean('BR_HIDDEN') }} as BR_HIDDEN,
    cast(CPL_PAYOUT as {{ dbt_utils.type_float() }}) as CPL_PAYOUT,
    cast(USER_ID as {{ dbt_utils.type_bigint() }}) as USER_ID,
    cast(CPL_REVENUE_PER_CLICK as {{ dbt_utils.type_float() }}) as CPL_REVENUE_PER_CLICK,
    cast(CPA_INCOME as {{ dbt_utils.type_float() }}) as CPA_INCOME,
    cast(UPDATED_BY as {{ dbt_utils.type_bigint() }}) as UPDATED_BY,
    cast(DEPOSIT as {{ dbt_utils.type_string() }}) as DEPOSIT,
    cast(REVSHARE_AFFILIATE as {{ dbt_utils.type_float() }}) as REVSHARE_AFFILIATE,
    cast(DEAL as {{ dbt_utils.type_string() }}) as DEAL,
    cast(FULL_REVIEW as {{ dbt_utils.type_string() }}) as FULL_REVIEW,
    cast(LINK_BANNER as {{ dbt_utils.type_string() }}) as LINK_BANNER,
    cast(CR_CLICK_TO_FTD_UNVERIFIED as {{ dbt_utils.type_float() }}) as CR_CLICK_TO_FTD_UNVERIFIED,
    cast(DESTINATION_URL as {{ dbt_utils.type_string() }}) as DESTINATION_URL,
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
    cast(CR_CLICK_TO_FTD_VERIFIED as {{ dbt_utils.type_float() }}) as CR_CLICK_TO_FTD_VERIFIED,
    cast(COLOR_BUTTON_TWO as {{ dbt_utils.type_string() }}) as COLOR_BUTTON_TWO,
    cast(PAST_NGR_PER_PLAYER_12M as {{ dbt_utils.type_float() }}) as PAST_NGR_PER_PLAYER_12M,
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
    cast(TOTAL_PAYOUT_VERIFIED as {{ dbt_utils.type_float() }}) as TOTAL_PAYOUT_VERIFIED,
    cast(COLOR_TEXT as {{ dbt_utils.type_string() }}) as COLOR_TEXT,
    cast(PUBLISHER_KEY_ID as {{ dbt_utils.type_bigint() }}) as PUBLISHER_KEY_ID,
    cast(CPL_PAYOUT_PER_CLICK as {{ dbt_utils.type_float() }}) as CPL_PAYOUT_PER_CLICK,
    cast(DISCLAIMER as {{ dbt_utils.type_string() }}) as DISCLAIMER,
    cast(TRACKING_ID as {{ dbt_utils.type_string() }}) as TRACKING_ID,
    cast(COUPON_TITLE as {{ dbt_utils.type_string() }}) as COUPON_TITLE,
    cast(REVSHARE_INCOME as {{ dbt_utils.type_float() }}) as REVSHARE_INCOME,
    cast(COUPON as {{ dbt_utils.type_string() }}) as COUPON,
    cast(LINK_TRACKING as {{ dbt_utils.type_string() }}) as LINK_TRACKING,
    cast(CPL_INCOME as {{ dbt_utils.type_float() }}) as CPL_INCOME,
    cast(FTDS_VERIFIED as {{ dbt_utils.type_bigint() }}) as FTDS_VERIFIED,
    cast(CPA_DIFF as {{ dbt_utils.type_float() }}) as CPA_DIFF,
    cast(BULLET_FOUR as {{ dbt_utils.type_string() }}) as BULLET_FOUR,
    {{ cast_to_boolean('DELETED') }} as DELETED,
    cast(CPA_PAYOUT as {{ dbt_utils.type_float() }}) as CPA_PAYOUT,
    {{ cast_to_boolean('BR_STATUS') }} as BR_STATUS,
    cast(LOGO_DARK as {{ dbt_utils.type_string() }}) as LOGO_DARK,
    cast(CPL_AFFILIATE as {{ dbt_utils.type_float() }}) as CPL_AFFILIATE,
    cast(FONT as {{ dbt_utils.type_string() }}) as FONT,
    cast(COLOR_BUTTON_ONE as {{ dbt_utils.type_string() }}) as COLOR_BUTTON_ONE,
    cast(BONUS as {{ dbt_utils.type_string() }}) as BONUS,
    cast(OPERATOR_ID as {{ dbt_utils.type_bigint() }}) as OPERATOR_ID,
    cast(REVSHARE_OPERATOR as {{ dbt_utils.type_float() }}) as REVSHARE_OPERATOR,
    cast(REVIEW_COUNT as {{ dbt_utils.type_bigint() }}) as REVIEW_COUNT,
    cast(LANGUAGE_ID as {{ dbt_utils.type_bigint() }}) as LANGUAGE_ID,
    cast(FINE_PRINT as {{ dbt_utils.type_string() }}) as FINE_PRINT,
    cast(CPA as {{ dbt_utils.type_bigint() }}) as CPA,
    cast(CR_SIGNUP_TO_FTD_UNVERIFIED as {{ dbt_utils.type_float() }}) as CR_SIGNUP_TO_FTD_UNVERIFIED,
    cast(CPC as {{ dbt_utils.type_bigint() }}) as CPC,
    cast(PREDICTED_NGR_PER_PLAYER_12M as {{ dbt_utils.type_float() }}) as PREDICTED_NGR_PER_PLAYER_12M,
    {{ cast_to_boolean('REVIEWED') }} as REVIEWED,
    cast(CPA_AFFILIATE as {{ dbt_utils.type_float() }}) as CPA_AFFILIATE,
    cast(OFFER_TYPE as {{ dbt_utils.type_string() }}) as OFFER_TYPE,
    cast(CAMPAIGN_ID as {{ dbt_utils.type_string() }}) as CAMPAIGN_ID,
    cast(PREVIEW_MOBILE as {{ dbt_utils.type_string() }}) as PREVIEW_MOBILE,
    cast(CR_SIGNUP_TO_FTD_VERIFIED as {{ dbt_utils.type_float() }}) as CR_SIGNUP_TO_FTD_VERIFIED,
    cast(LINK_OFFER as {{ dbt_utils.type_string() }}) as LINK_OFFER,
    cast(RIBBON as {{ dbt_utils.type_string() }}) as RIBBON,
    cast(PREVIEW_LARGE as {{ dbt_utils.type_string() }}) as PREVIEW_LARGE,
    cast(CREATED_BY as {{ dbt_utils.type_bigint() }}) as CREATED_BY,
    cast(DEPOSITS as {{ dbt_utils.type_bigint() }}) as DEPOSITS,
    cast(BRAND_ID as {{ dbt_utils.type_bigint() }}) as BRAND_ID,
    cast(TOTAL_REVENUE_PER_CLICK as {{ dbt_utils.type_float() }}) as TOTAL_REVENUE_PER_CLICK,
    cast(TOTAL_INCOME as {{ dbt_utils.type_float() }}) as TOTAL_INCOME,
    cast(LINK_TERMS as {{ dbt_utils.type_string() }}) as LINK_TERMS,
    cast(PARENT_ID as {{ dbt_utils.type_bigint() }}) as PARENT_ID,
    cast(TOTAL_PAYOUT_UNVERIFIED as {{ dbt_utils.type_float() }}) as TOTAL_PAYOUT_UNVERIFIED,
    cast(REVSHARE_REVENUE as {{ dbt_utils.type_float() }}) as REVSHARE_REVENUE,
    cast(REVSHARE_PAYOUT as {{ dbt_utils.type_float() }}) as REVSHARE_PAYOUT,
    cast(CR_CLICK_TO_SIGNUP as {{ dbt_utils.type_float() }}) as CR_CLICK_TO_SIGNUP,
    cast(LICENSED_STATES as {{ dbt_utils.type_string() }}) as LICENSED_STATES,
    cast(PAST_NGR_PER_PLAYER_6M as {{ dbt_utils.type_float() }}) as PAST_NGR_PER_PLAYER_6M,
    cast(PREVIEW as {{ dbt_utils.type_string() }}) as PREVIEW,
    cast(PREVIEW_SMALL as {{ dbt_utils.type_string() }}) as PREVIEW_SMALL,
    cast(BULLET_ONE as {{ dbt_utils.type_string() }}) as BULLET_ONE,
    cast(PREDICTED_NGR_PER_PLAYER_1M as {{ dbt_utils.type_float() }}) as PREDICTED_NGR_PER_PLAYER_1M,
    cast(CTA_ONE as {{ dbt_utils.type_string() }}) as CTA_ONE,
    cast(BONUS_SUB as {{ dbt_utils.type_string() }}) as BONUS_SUB,
    cast(LOGO_ALT as {{ dbt_utils.type_string() }}) as LOGO_ALT,
    cast(CPA_OPERATOR as {{ dbt_utils.type_float() }}) as CPA_OPERATOR,
    cast(TITLE as {{ dbt_utils.type_string() }}) as TITLE,
    cast(OFFER_ORDER as {{ dbt_utils.type_bigint() }}) as OFFER_ORDER,
    cast(CAMPAIGN_NAME as {{ dbt_utils.type_string() }}) as CAMPAIGN_NAME,
    cast(TOTAL_PAYOUT_PER_CLICK_UNVERIFIED as {{ dbt_utils.type_float() }}) as TOTAL_PAYOUT_PER_CLICK_UNVERIFIED,
    cast(LOGO_LIGHT as {{ dbt_utils.type_string() }}) as LOGO_LIGHT,
    cast(NGR_CONFIDENCE as {{ dbt_utils.type_float() }}) as NGR_CONFIDENCE,
    cast(FTDS_UNVERIFIED as {{ dbt_utils.type_bigint() }}) as FTDS_UNVERIFIED,
    cast(OPERATOR_ACCOUNT_ID as {{ dbt_utils.type_bigint() }}) as OPERATOR_ACCOUNT_ID,
    cast(REVSHARE as {{ dbt_utils.type_bigint() }}) as REVSHARE,
    cast(BULLET_TWO as {{ dbt_utils.type_string() }}) as BULLET_TWO,
    cast(LINK_REVIEW as {{ dbt_utils.type_string() }}) as LINK_REVIEW,
    cast(CTA_TWO as {{ dbt_utils.type_string() }}) as CTA_TWO,
    cast(CPL_INCOME_PER_CLICK as {{ dbt_utils.type_float() }}) as CPL_INCOME_PER_CLICK,
    cast(REVSHARE_PAYOUT_PER_CLICK as {{ dbt_utils.type_float() }}) as REVSHARE_PAYOUT_PER_CLICK,
    cast(STARS as {{ dbt_utils.type_float() }}) as STARS,
    cast(CPA_REVENUE as {{ dbt_utils.type_float() }}) as CPA_REVENUE,
    cast(SCORE_TITLE as {{ dbt_utils.type_string() }}) as SCORE_TITLE,
    cast(CPL_REVENUE as {{ dbt_utils.type_float() }}) as CPL_REVENUE,
    cast(CPA_PAYOUT_PER_CLICK as {{ dbt_utils.type_float() }}) as CPA_PAYOUT_PER_CLICK,
    cast(COLOR_BACKGROUND as {{ dbt_utils.type_string() }}) as COLOR_BACKGROUND,
    cast(TOTAL_REVENUE as {{ dbt_utils.type_float() }}) as TOTAL_REVENUE,
    cast(LICENSED_COUNTRY as {{ dbt_utils.type_string() }}) as LICENSED_COUNTRY,
    cast(CLICKS as {{ dbt_utils.type_bigint() }}) as CLICKS,
    cast(CURRENCY_ID as {{ dbt_utils.type_bigint() }}) as CURRENCY_ID,
    cast(EXTERNAL_REVIEW_PAGE as {{ dbt_utils.type_string() }}) as EXTERNAL_REVIEW_PAGE,
    cast(TOTAL_PAYOUT_PER_CLICK_VERIFIED as {{ dbt_utils.type_float() }}) as TOTAL_PAYOUT_PER_CLICK_VERIFIED,
    S3_PATH,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('OFFERS_AB1') }}
-- OFFERS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

