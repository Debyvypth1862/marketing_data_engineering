{{ config(
    cluster_by = ["_AIRBYTE_ACTIVE_ROW", "_AIRBYTE_UNIQUE_KEY_SCD", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY_SCD",
    database = env_var('SCD_DATABASE'),
    schema = "BRC",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='POSTBACK_TRACKING'
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
                                where 1=1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('POSTBACK_TRACKING')) }}
                            ) recent_records
                            left join (
                                select _AIRBYTE_UNIQUE_KEY as unique_key, count(_AIRBYTE_UNIQUE_KEY) as active_count
                                from {{ this }}
                                where _AIRBYTE_ACTIVE_ROW = 1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('POSTBACK_TRACKING')) }}
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
                    "drop view {{ this.database }}.BRC.POSTBACK_TRACKING_STG"],
                    
    tags = [ "top-level" ]
) }}
-- depends_on: ref('POSTBACK_TRACKING_STG')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('POSTBACK_TRACKING_STG')  }}
    -- POSTBACK_TRACKING from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_TRACKING') }}
    where 1 = 1
    {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
),
new_data_ids as (
    -- build a subset of _AIRBYTE_UNIQUE_KEY from rows that are new
    select distinct
        POST_ID as _AIRBYTE_UNIQUE_KEY
    from new_data
),
empty_new_data as (
    -- build an empty table to only keep the table's column types
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    -- retrieve "incomplete old" data that needs to be updated with an end date because of new changes
    select
        {{ star_intersect(ref('POSTBACK_TRACKING_STG'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._AIRBYTE_UNIQUE_KEY = new_data_ids._AIRBYTE_UNIQUE_KEY
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._AIRBYTE_AB_ID = inc_data._AIRBYTE_AB_ID
    where _AIRBYTE_ACTIVE_ROW = 1
),
input_data as (
    select {{ dbt_utils.star(ref('POSTBACK_TRACKING_STG')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('POSTBACK_TRACKING_STG')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('POSTBACK_TRACKING_STG')  }}
    -- POSTBACK_TRACKING from {{ source('BRC', '_AIRBYTE_RAW_POSTBACK_TRACKING') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
      
      POST_ID as _AIRBYTE_UNIQUE_KEY,
      POST_SUBID4,
      POST_FTD_TIMESTAMP,
      POST_FBCLID,
      POST_SUBID5,
      POST_OW_ID,
      POST_AFFILIATE_ID,
      POST_IP,
      POST_CLICK_DATE,
      POST_PAGE_LOCATION,
      POST_FK_TRACKER,
      POST_CLICK_TIMESTAMP,
      POST_SUBID2,
      POST_FK_CAMT_ID,
      POST_SUBID3,
      POST_ADGROUPID,
      POST_UTM_CONTENT,
      POST_UTM_ID,
      POST_SUBID,
      POST_GA4_DEVICE_ID,
      POST_MODIFIED_TIMESTAMP,
      POST_CPA_TIMESTAMP,
      POST_UTM_SOURCE,
      POST_SIGNUP_DATE,
      POST_SITE_MEMBER_ID,
      POST_UTM_CAMPAIGN,
      POST_CAMPAIGNID,
      POST_UTM_MEDIUM,
      POST_ENV,
      POST_KEYWORD,
      POST_CREATIVE,
      POST_FTD_DATE,
      POST_PAGE,
      POST_GCLID,
      POST_SIGNUP_TIMESTAMP,
      POST_UTM_TERM,
      POST_3RD_PARTY_CLICKID,
      POST_ID,
      POST_ADACCOUNTID,
      POST_MARKETING_SITE_ID,
      POST_CLICKID,
      POST_CPA_DATE,
      POST_TEST_VARIATION,
      POST_APP_INSTANCE_ID,
      POST_FIREBASE_APP_ID,
      S3_PATH,
      POST_CLICK_DATE as _AIRBYTE_START_AT,
      lag(POST_CLICK_DATE) over (
        partition by cast(POST_ID as {{ dbt_utils.type_string() }})
        order by
            POST_CLICK_DATE is null asc,
            POST_CLICK_DATE desc,
            _AIRBYTE_EMITTED_AT desc
      ) as _AIRBYTE_END_AT,
      case when row_number() over (
        partition by cast(POST_ID as {{ dbt_utils.type_string() }})
        order by
            POST_CLICK_DATE is null asc,
            POST_CLICK_DATE desc,
            _AIRBYTE_EMITTED_AT desc
      ) = 1 then 1 else 0 end as _AIRBYTE_ACTIVE_ROW,
      _AIRBYTE_AB_ID,
      _AIRBYTE_EMITTED_AT,
      _AIRBYTE_POSTBACK_TRACKING_HASHID
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
    POST_SUBID4,
    POST_FTD_TIMESTAMP,
    POST_FBCLID,
    POST_SUBID5,
    POST_OW_ID,
    POST_AFFILIATE_ID,
    POST_IP,
    POST_CLICK_DATE,
    POST_PAGE_LOCATION,
    POST_FK_TRACKER,
    POST_CLICK_TIMESTAMP,
    POST_SUBID2,
    POST_FK_CAMT_ID,
    POST_SUBID3,
    POST_ADGROUPID,
    POST_UTM_CONTENT,
    POST_UTM_ID,
    POST_SUBID,
    POST_GA4_DEVICE_ID,
    POST_MODIFIED_TIMESTAMP,
    POST_CPA_TIMESTAMP,
    POST_UTM_SOURCE,
    POST_SIGNUP_DATE,
    POST_SITE_MEMBER_ID,
    POST_UTM_CAMPAIGN,
    POST_CAMPAIGNID,
    POST_UTM_MEDIUM,
    POST_ENV,
    POST_KEYWORD,
    POST_CREATIVE,
    POST_FTD_DATE,
    POST_PAGE,
    POST_GCLID,
    POST_SIGNUP_TIMESTAMP,
    POST_UTM_TERM,
    POST_3RD_PARTY_CLICKID,
    POST_ID,
    POST_ADACCOUNTID,
    POST_MARKETING_SITE_ID,
    POST_CLICKID,
    POST_CPA_DATE,
    POST_TEST_VARIATION,
    POST_APP_INSTANCE_ID,
    POST_FIREBASE_APP_ID,
    S3_PATH,
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    CURRENT_TIMESTAMP() as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_POSTBACK_TRACKING_HASHID
from dedup_data where _AIRBYTE_ROW_NUM = 1

