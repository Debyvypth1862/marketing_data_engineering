{{ config(
    cluster_by = ["_AIRBYTE_ACTIVE_ROW", "_AIRBYTE_UNIQUE_KEY_SCD", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY_SCD",
    database = env_var('SCD_DATABASE'),
    schema = "REDTRACK",
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='CONVERSIONS'
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
                                where 1=1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('CONVERSIONS')) }}
                            ) recent_records
                            left join (
                                select _AIRBYTE_UNIQUE_KEY as unique_key, count(_AIRBYTE_UNIQUE_KEY) as active_count
                                from {{ this }}
                                where _AIRBYTE_ACTIVE_ROW = 1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', adapter.quote(this.schema) + '.' + adapter.quote('CONVERSIONS')) }}
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
                    "drop view {{ ref('CONVERSIONS_STG') }}"],
    tags = [ "top-level" ]
) }}
-- depends_on: ref('CONVERSIONS_STG')
with
{% if is_incremental() %}
new_data as (
    -- retrieve incremental "new" data
    select
        *
    from {{ ref('CONVERSIONS_STG')  }}
    -- CONVERSIONS from {{ source('REDTRACK', '_AIRBYTE_RAW_CONVERSIONS') }}
    where 1 = 1
    {{ incremental_clause('PROCESSED_DATE', this) }}
),
new_data_ids as (
    -- build a subset of _AIRBYTE_UNIQUE_KEY from rows that are new
    select distinct
        {{ dbt_utils.surrogate_key([
            "LEFT(CONV_TIME, 19)",
            'CLICKID',
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
        {{ star_intersect(ref('CONVERSIONS_STG'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    -- make a join with new_data using primary key to filter active data that need to be updated only
    join new_data_ids on this_data._AIRBYTE_UNIQUE_KEY = new_data_ids._AIRBYTE_UNIQUE_KEY
    -- force left join to NULL values (we just need to transfer column types only for the star_intersect macro on schema changes)
    left join empty_new_data as inc_data on this_data._AIRBYTE_AB_ID = inc_data._AIRBYTE_AB_ID
    where _AIRBYTE_ACTIVE_ROW = 1
),
input_data as (
    select {{ dbt_utils.star(ref('CONVERSIONS_STG')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('CONVERSIONS_STG')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('CONVERSIONS_STG')  }}
    -- CONVERSIONS from {{ source('REDTRACK', '_AIRBYTE_RAW_CONVERSIONS') }}
),
{% endif %}
scd_data as (
    -- SQL model to build a Type 2 Slowly Changing Dimension (SCD) table for each record identified by their primary key
    select
        {{ dbt_utils.surrogate_key([
            "LEFT(CONV_TIME, 19)",
            'CLICKID',
            'ID',
        ]) }} as _AIRBYTE_UNIQUE_KEY,
        BROWSER,
        CAMPAIGN,
        CAMPAIGN_ID,
        CITY,
        CLICKID,
        CONNECTION_TYPE,
        CONV_TIME,
        COST,
        COST_DEFAULT,
        COST_SOURCE,
        COUNTRY,
        COUPON,
        CREATED_AT,
        CURRENCY,
        DEDUPLICATE_TOKEN,
        DEEPLINK,
        DEVICE,
        DEVICE_BRAND,
        DEVICE_FULLNAME,
        DUPLICATE_STATUS,
        "EVENT",
        EXTERNAL_ID,
        FINGERPRINT,
        ID,
        IP,
        IS_TRANSACTION,
        ISP,
        LANDING,
        LANDING_ID,
        NETWORK,
        OFFER,
        OFFER_ID,
        ORDERS,
        OS,
        P_SUB1,
        P_SUB10,
        P_SUB11,
        P_SUB12,
        P_SUB13,
        P_SUB14,
        P_SUB15,
        P_SUB16,
        P_SUB17,
        P_SUB18,
        P_SUB19,
        P_SUB2,
        P_SUB20,
        P_SUB3,
        P_SUB4,
        P_SUB5,
        P_SUB6,
        P_SUB7,
        P_SUB8,
        P_SUB9,
        PAGE,
        PAGE_URL,
        PAYOUT,
        PAYOUT_DEFAULT,
        PAYOUT_NETWORK,
        POSTBACK_IP,
        PRELANDING,
        PRELANDING_ID,
        PROGRAM_ID,
        PUB_REVENUE,
        PUB_REVENUE_DEFAULT,
        PUB_REVENUE_SOURCE,
        REF_ID,
        REFERER,
        RT_AD,
        RT_AD_ID,
        RT_ADGROUP,
        RT_ADGROUP_ID,
        RT_CAMPAIGN,
        RT_CAMPAIGN_ID,
        RT_KEYWORD,
        RT_MEDIUM,
        RT_PLACEMENT,
        RT_PLACEMENT_HASHED,
        RT_PLACEMENT_ID,
        RT_ROLE_1,
        RT_ROLE_2,
        RT_SOURCE,
        SERVER,
        SOURCE,
        SOURCE_ID,
        STATUS,
        SUB1,
        SUB10,
        SUB11,
        SUB12,
        SUB13,
        SUB14,
        SUB15,
        SUB16,
        SUB17,
        SUB18,
        SUB19,
        SUB2,
        SUB20,
        SUB3,
        SUB4,
        SUB5,
        SUB6,
        SUB7,
        SUB8,
        SUB9,
        TRACK_TIME,
        TYPE,
        TYPE1,
        TYPE10,
        TYPE11,
        TYPE12,
        TYPE13,
        TYPE14,
        TYPE15,
        TYPE16,
        TYPE17,
        TYPE18,
        TYPE19,
        TYPE2,
        TYPE20,
        TYPE21,
        TYPE22,
        TYPE23,
        TYPE24,
        TYPE25,
        TYPE26,
        TYPE27,
        TYPE28,
        TYPE29,
        TYPE3,
        TYPE30,
        TYPE31,
        TYPE32,
        TYPE33,
        TYPE34,
        TYPE35,
        TYPE36,
        TYPE37,
        TYPE38,
        TYPE39,
        TYPE4,
        TYPE40,
        TYPE5,
        TYPE6,
        TYPE7,
        TYPE8,
        TYPE9,
        USER_AGENT,
        CONV_TIME as _AIRBYTE_START_AT,
        lag(CONV_TIME) over (
            partition by LEFT(CONV_TIME, 19), CLICKID, ID
            order by
                CONV_TIME is null asc,
                CONV_TIME desc,
                _AIRBYTE_EMITTED_AT desc
        ) as _AIRBYTE_END_AT,
        case when row_number() over (
            partition by LEFT(CONV_TIME, 19), CLICKID, ID
            order by
                CONV_TIME is null asc,
                CONV_TIME desc,
                _AIRBYTE_EMITTED_AT desc
        ) = 1 then 1 else 0 end as _AIRBYTE_ACTIVE_ROW,
        _AIRBYTE_AB_ID,
        _AIRBYTE_EMITTED_AT,
        S3_PATH,
        _AIRBYTE_CONVERSIONS_HASHID,
        PROCESSED_DATE
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
    BROWSER,
    CAMPAIGN,
    CAMPAIGN_ID,
    CITY,
    CLICKID,
    CONNECTION_TYPE,
    CONV_TIME,
    COST,
    COST_DEFAULT,
    COST_SOURCE,
    COUNTRY,
    COUPON,
    CREATED_AT,
    CURRENCY,
    DEDUPLICATE_TOKEN,
    DEEPLINK,
    DEVICE,
    DEVICE_BRAND,
    DEVICE_FULLNAME,
    DUPLICATE_STATUS,
    EVENT,
    EXTERNAL_ID,
    FINGERPRINT,
    ID,
    IP,
    IS_TRANSACTION,
    ISP,
    LANDING,
    LANDING_ID,
    NETWORK,
    OFFER,
    OFFER_ID,
    ORDERS,
    OS,
    P_SUB1,
    P_SUB10,
    P_SUB11,
    P_SUB12,
    P_SUB13,
    P_SUB14,
    P_SUB15,
    P_SUB16,
    P_SUB17,
    P_SUB18,
    P_SUB19,
    P_SUB2,
    P_SUB20,
    P_SUB3,
    P_SUB4,
    P_SUB5,
    P_SUB6,
    P_SUB7,
    P_SUB8,
    P_SUB9,
    PAGE,
    PAGE_URL,
    PAYOUT,
    PAYOUT_DEFAULT,
    PAYOUT_NETWORK,
    POSTBACK_IP,
    PRELANDING,
    PRELANDING_ID,
    PROGRAM_ID,
    PUB_REVENUE,
    PUB_REVENUE_DEFAULT,
    PUB_REVENUE_SOURCE,
    REF_ID,
    REFERER,
    RT_AD,
    RT_AD_ID,
    RT_ADGROUP,
    RT_ADGROUP_ID,
    RT_CAMPAIGN,
    RT_CAMPAIGN_ID,
    RT_KEYWORD,
    RT_MEDIUM,
    RT_PLACEMENT,
    RT_PLACEMENT_HASHED,
    RT_PLACEMENT_ID,
    RT_ROLE_1,
    RT_ROLE_2,
    RT_SOURCE,
    SERVER,
    SOURCE,
    SOURCE_ID,
    STATUS,
    SUB1,
    SUB10,
    SUB11,
    SUB12,
    SUB13,
    SUB14,
    SUB15,
    SUB16,
    SUB17,
    SUB18,
    SUB19,
    SUB2,
    SUB20,
    SUB3,
    SUB4,
    SUB5,
    SUB6,
    SUB7,
    SUB8,
    SUB9,
    TRACK_TIME,
    TYPE,
    TYPE1,
    TYPE10,
    TYPE11,
    TYPE12,
    TYPE13,
    TYPE14,
    TYPE15,
    TYPE16,
    TYPE17,
    TYPE18,
    TYPE19,
    TYPE2,
    TYPE20,
    TYPE21,
    TYPE22,
    TYPE23,
    TYPE24,
    TYPE25,
    TYPE26,
    TYPE27,
    TYPE28,
    TYPE29,
    TYPE3,
    TYPE30,
    TYPE31,
    TYPE32,
    TYPE33,
    TYPE34,
    TYPE35,
    TYPE36,
    TYPE37,
    TYPE38,
    TYPE39,
    TYPE4,
    TYPE40,
    TYPE5,
    TYPE6,
    TYPE7,
    TYPE8,
    TYPE9,
    USER_AGENT,
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_CONVERSIONS_HASHID,
    PROCESSED_DATE
from dedup_data 
where _AIRBYTE_ROW_NUM = 1

