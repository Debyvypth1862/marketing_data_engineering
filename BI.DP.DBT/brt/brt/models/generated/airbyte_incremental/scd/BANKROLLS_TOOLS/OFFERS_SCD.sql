{{ config(
    cluster_by = ["_AIRBYTE_ACTIVE_ROW", "_AIRBYTE_UNIQUE_KEY_SCD", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY_SCD",
    database = env_var('SCD_DATABASE'),
    schema = "BRT",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='OFFERS_'
                        )
                    %}
                    {#
                    If the final table doesn't exist, then obviously we can't delete anything from it.
                    Also, after a reset, the final table is created without the _airbyte_unique_key column (this column is created during the first sync)
                    So skip this deletion if the column doesn't exist. (in this case, the table is guaranteed to be empty anyway)
                    #}
                    {%
                    if final_table_relation is not none and '_AIRBYTE_UNIQUE_KEY' in adapter.get_columns_in_relation(final_table_relation)|map(attribute='name')
                    %}
                    -- Delete records which are no longer active:
                    -- This query is equivalent, but the left join version is more performant:
                    -- delete from final_table where unique_key in (
                    --     select unique_key from scd_table where 1 = 1 <incremental_clause(normalized_at, final_table)>
                    -- ) and unique_key not in (
                    --     select unique_key from scd_table where active_row = 1 <incremental_clause(normalized_at, final_table)>
                    -- )
                    -- We're incremental against normalized_at rather than emitted_at because we need to fetch the SCD
                    -- entries that were _updated_ recently. This is because a deleted record will have an SCD record
                    -- which was emitted a long time ago, but recently re-normalized to have active_row = 0.
                    delete from {{ final_table_relation }} where {{ final_table_relation }}._AIRBYTE_UNIQUE_KEY in (
                        select recent_records.unique_key
                        from (
                                select distinct _AIRBYTE_UNIQUE_KEY as unique_key
                                from {{ this }}
                                where 1=1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('POSTBACK_3RD_PARTY_CLICK_LOG')) }}
                            ) recent_records
                            left join (
                                select _AIRBYTE_UNIQUE_KEY as unique_key, count(_AIRBYTE_UNIQUE_KEY) as active_count
                                from {{ this }}
                                where _AIRBYTE_ACTIVE_ROW = 1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('POSTBACK_3RD_PARTY_CLICK_LOG')) }}
                                group by _AIRBYTE_UNIQUE_KEY
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    -- We have to have a non-empty query, so just do a noop delete
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ",
                    "UPDATE {{ env_var('snowflake_db_dbt') }}.{{ env_var('snowflake_schema_dbt') }}.{{ env_var('snowflake_table_dbt') }}
                    SET IS_PROCESSED = TRUE, PROCESSED_AT= {{ current_timestamp() }}, STATUS = 'Success',RECORD_COUNT = DBT_INTERNAL_SOURCE.RECORD_COUNT
                    FROM (SELECT S3_PATH, count(*) as RECORD_COUNT
                                FROM {{ this }} WHERE _AIRBYTE_EMITTED_AT >= CURRENT_DATE GROUP BY 1 ) as DBT_INTERNAL_SOURCE
                        WHERE PATH = DBT_INTERNAL_SOURCE.S3_PATH",
                     "drop view {{ this.database }}.BRT.OFFERS_STG"],
                    
    tags = [ "top-level" ]
) }}
-- depends_on: ref('OFFERS_STG')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('OFFERS_STG')  }}
    -- OFFERS from {{ source('BRT', '_AIRBYTE_RAW_OFFERS') }}
    where 1 = 1
    {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
),
new_data_ids as (
    -- build a subset of _AIRBYTE_UNIQUE_KEY from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            'ID',
        ]) }} as _AIRBYTE_UNIQUE_KEY
    from new_data
),
empty_new_data as (
    -- build an empty table to only keep the table's column types
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('OFFERS_STG'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._AIRBYTE_UNIQUE_KEY = new_data_ids._AIRBYTE_UNIQUE_KEY
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._AIRBYTE_AB_ID = inc_data._AIRBYTE_AB_ID
    where _AIRBYTE_ACTIVE_ROW = 1
),
input_data as (
    select {{ dbt_utils.star(ref('OFFERS_STG')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('OFFERS_STG')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('OFFERS_STG')  }}
    -- OFFERS from {{ source('BRT', '_AIRBYTE_RAW_OFFERS') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      {{ dbt_utils.surrogate_key([
      'ID',
      ]) }} as _AIRBYTE_UNIQUE_KEY,
      NGR_INSUFFICIENT_DATA,
      CPL_OPERATOR,
      REVSHARE_DIFF,
      BONUS_OFFER_LINE_3,
      BONUS_OFFER_LINE_6,
      REVIEW,
      BONUS_OFFER_LINE_5,
      BONUS_OFFER_LINE_4,
      BULLET_THREE,
      ID,
      PAST_NGR_PER_PLAYER_1M,
      NGR_PROCESSED_AT,
      CPA_REVENUE_PER_CLICK,
      UPDATE_APPROVED,
      REVIEW_COUNT_TITLE,
      SIGN_UPS,
      CPL_DIFF,
      TOTAL_INCOME_PER_CLICK,
      PREDICTED_NGR_PER_PLAYER_6M,
      ACTIVE,
      BASELINE,
      LICENSE_STATUS,
      TRAFFIC_SOURCE,
      BR_HIDDEN,
      CPL_PAYOUT,
      USER_ID,
      CPL_REVENUE_PER_CLICK,
      CPA_INCOME,
      UPDATED_BY,
      DEPOSIT,
      REVSHARE_AFFILIATE,
      DEAL,
      FULL_REVIEW,
      LINK_BANNER,
      CR_CLICK_TO_FTD_UNVERIFIED,
      DESTINATION_URL,
      REVSHARE_INCOME_PER_CLICK,
      REVSHARE_REVENUE_PER_CLICK,
      CREATED_AT,
      CR_CLICK_TO_FTD_VERIFIED,
      COLOR_BUTTON_TWO,
      PAST_NGR_PER_PLAYER_12M,
      UPDATED_AT,
      CPA_INCOME_PER_CLICK,
      TOTAL_PAYOUT_VERIFIED,
      COLOR_TEXT,
      PUBLISHER_KEY_ID,
      CPL_PAYOUT_PER_CLICK,
      DISCLAIMER,
      TRACKING_ID,
      COUPON_TITLE,
      REVSHARE_INCOME,
      COUPON,
      LINK_TRACKING,
      CPL_INCOME,
      FTDS_VERIFIED,
      CPA_DIFF,
      BULLET_FOUR,
      DELETED,
      CPA_PAYOUT,
      BR_STATUS,
      LOGO_DARK,
      CPL_AFFILIATE,
      FONT,
      COLOR_BUTTON_ONE,
      BONUS,
      OPERATOR_ID,
      REVSHARE_OPERATOR,
      REVIEW_COUNT,
      LANGUAGE_ID,
      FINE_PRINT,
      CPA,
      CR_SIGNUP_TO_FTD_UNVERIFIED,
      CPC,
      PREDICTED_NGR_PER_PLAYER_12M,
      REVIEWED,
      CPA_AFFILIATE,
      OFFER_TYPE,
      CAMPAIGN_ID,
      PREVIEW_MOBILE,
      CR_SIGNUP_TO_FTD_VERIFIED,
      LINK_OFFER,
      RIBBON,
      PREVIEW_LARGE,
      CREATED_BY,
      DEPOSITS,
      BRAND_ID,
      TOTAL_REVENUE_PER_CLICK,
      TOTAL_INCOME,
      LINK_TERMS,
      PARENT_ID,
      TOTAL_PAYOUT_UNVERIFIED,
      REVSHARE_REVENUE,
      REVSHARE_PAYOUT,
      CR_CLICK_TO_SIGNUP,
      LICENSED_STATES,
      PAST_NGR_PER_PLAYER_6M,
      PREVIEW,
      PREVIEW_SMALL,
      BULLET_ONE,
      PREDICTED_NGR_PER_PLAYER_1M,
      CTA_ONE,
      BONUS_SUB,
      LOGO_ALT,
      CPA_OPERATOR,
      TITLE,
      OFFER_ORDER,
      CAMPAIGN_NAME,
      TOTAL_PAYOUT_PER_CLICK_UNVERIFIED,
      LOGO_LIGHT,
      NGR_CONFIDENCE,
      FTDS_UNVERIFIED,
      OPERATOR_ACCOUNT_ID,
      REVSHARE,
      BULLET_TWO,
      LINK_REVIEW,
      CTA_TWO,
      CPL_INCOME_PER_CLICK,
      REVSHARE_PAYOUT_PER_CLICK,
      STARS,
      CPA_REVENUE,
      SCORE_TITLE,
      CPL_REVENUE,
      CPA_PAYOUT_PER_CLICK,
      COLOR_BACKGROUND,
      TOTAL_REVENUE,
      LICENSED_COUNTRY,
      CLICKS,
      CURRENCY_ID,
      EXTERNAL_REVIEW_PAGE,
      TOTAL_PAYOUT_PER_CLICK_VERIFIED,
      S3_PATH,
      UPDATED_AT as _AIRBYTE_START_AT,
      lag(UPDATED_AT) over (
        partition by cast(ID as {{ dbt_utils.type_string() }})
        order by
            UPDATED_AT is null asc,
            UPDATED_AT desc,
            _AIRBYTE_EMITTED_AT desc
      ) as _AIRBYTE_END_AT,
      case when row_number() over (
        partition by cast(ID as {{ dbt_utils.type_string() }})
        order by
            UPDATED_AT is null asc,
            UPDATED_AT desc,
            _AIRBYTE_EMITTED_AT desc
      ) = 1 then 1 else 0 end as _AIRBYTE_ACTIVE_ROW,
      _AIRBYTE_AB_ID,
      _AIRBYTE_EMITTED_AT,
      _AIRBYTE_OFFERS_HASHID
    from input_data
),
dedup_data as (
    select
        -- we need to ensure de-duplicated rows for merge/update queries
        -- additionally, we generate a unique key for the scd table
        row_number() over (
            partition by
                _AIRBYTE_UNIQUE_KEY,
                _AIRBYTE_START_AT,
                _AIRBYTE_EMITTED_AT
            order by _AIRBYTE_ACTIVE_ROW desc, _AIRBYTE_AB_ID
        ) as _AIRBYTE_ROW_NUM,
        {{ dbt_utils.surrogate_key([
          '_AIRBYTE_UNIQUE_KEY',
          '_AIRBYTE_START_AT',
          '_AIRBYTE_EMITTED_AT'
        ]) }} as _AIRBYTE_UNIQUE_KEY_SCD,
        scd_data.*
    from scd_data
)
select
    _AIRBYTE_UNIQUE_KEY,
    _AIRBYTE_UNIQUE_KEY_SCD,
    NGR_INSUFFICIENT_DATA,
    CPL_OPERATOR,
    REVSHARE_DIFF,
    BONUS_OFFER_LINE_3,
    BONUS_OFFER_LINE_6,
    REVIEW,
    BONUS_OFFER_LINE_5,
    BONUS_OFFER_LINE_4,
    BULLET_THREE,
    ID,
    PAST_NGR_PER_PLAYER_1M,
    NGR_PROCESSED_AT,
    CPA_REVENUE_PER_CLICK,
    UPDATE_APPROVED,
    REVIEW_COUNT_TITLE,
    SIGN_UPS,
    CPL_DIFF,
    TOTAL_INCOME_PER_CLICK,
    PREDICTED_NGR_PER_PLAYER_6M,
    ACTIVE,
    BASELINE,
    LICENSE_STATUS,
    TRAFFIC_SOURCE,
    BR_HIDDEN,
    CPL_PAYOUT,
    USER_ID,
    CPL_REVENUE_PER_CLICK,
    CPA_INCOME,
    UPDATED_BY,
    DEPOSIT,
    REVSHARE_AFFILIATE,
    DEAL,
    FULL_REVIEW,
    LINK_BANNER,
    CR_CLICK_TO_FTD_UNVERIFIED,
    DESTINATION_URL,
    REVSHARE_INCOME_PER_CLICK,
    REVSHARE_REVENUE_PER_CLICK,
    CREATED_AT,
    CR_CLICK_TO_FTD_VERIFIED,
    COLOR_BUTTON_TWO,
    PAST_NGR_PER_PLAYER_12M,
    UPDATED_AT,
    CPA_INCOME_PER_CLICK,
    TOTAL_PAYOUT_VERIFIED,
    COLOR_TEXT,
    PUBLISHER_KEY_ID,
    CPL_PAYOUT_PER_CLICK,
    DISCLAIMER,
    TRACKING_ID,
    COUPON_TITLE,
    REVSHARE_INCOME,
    COUPON,
    LINK_TRACKING,
    CPL_INCOME,
    FTDS_VERIFIED,
    CPA_DIFF,
    BULLET_FOUR,
    DELETED,
    CPA_PAYOUT,
    BR_STATUS,
    LOGO_DARK,
    CPL_AFFILIATE,
    FONT,
    COLOR_BUTTON_ONE,
    BONUS,
    OPERATOR_ID,
    REVSHARE_OPERATOR,
    REVIEW_COUNT,
    LANGUAGE_ID,
    FINE_PRINT,
    CPA,
    CR_SIGNUP_TO_FTD_UNVERIFIED,
    CPC,
    PREDICTED_NGR_PER_PLAYER_12M,
    REVIEWED,
    CPA_AFFILIATE,
    OFFER_TYPE,
    CAMPAIGN_ID,
    PREVIEW_MOBILE,
    CR_SIGNUP_TO_FTD_VERIFIED,
    LINK_OFFER,
    RIBBON,
    PREVIEW_LARGE,
    CREATED_BY,
    DEPOSITS,
    BRAND_ID,
    TOTAL_REVENUE_PER_CLICK,
    TOTAL_INCOME,
    LINK_TERMS,
    PARENT_ID,
    TOTAL_PAYOUT_UNVERIFIED,
    REVSHARE_REVENUE,
    REVSHARE_PAYOUT,
    CR_CLICK_TO_SIGNUP,
    LICENSED_STATES,
    PAST_NGR_PER_PLAYER_6M,
    PREVIEW,
    PREVIEW_SMALL,
    BULLET_ONE,
    PREDICTED_NGR_PER_PLAYER_1M,
    CTA_ONE,
    BONUS_SUB,
    LOGO_ALT,
    CPA_OPERATOR,
    TITLE,
    OFFER_ORDER,
    CAMPAIGN_NAME,
    TOTAL_PAYOUT_PER_CLICK_UNVERIFIED,
    LOGO_LIGHT,
    NGR_CONFIDENCE,
    FTDS_UNVERIFIED,
    OPERATOR_ACCOUNT_ID,
    REVSHARE,
    BULLET_TWO,
    LINK_REVIEW,
    CTA_TWO,
    CPL_INCOME_PER_CLICK,
    REVSHARE_PAYOUT_PER_CLICK,
    STARS,
    CPA_REVENUE,
    SCORE_TITLE,
    CPL_REVENUE,
    CPA_PAYOUT_PER_CLICK,
    COLOR_BACKGROUND,
    TOTAL_REVENUE,
    LICENSED_COUNTRY,
    CLICKS,
    CURRENCY_ID,
    EXTERNAL_REVIEW_PAGE,
    TOTAL_PAYOUT_PER_CLICK_VERIFIED,
    S3_PATH,
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OFFERS_HASHID
from dedup_data where _AIRBYTE_ROW_NUM = 1

