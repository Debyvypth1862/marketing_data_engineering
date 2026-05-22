{{ config(
    cluster_by = ["_AIRBYTE_ACTIVE_ROW", "_AIRBYTE_UNIQUE_KEY_SCD", "_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY_SCD",
    database = env_var('INTM_DATABASE', 'INTM'),
    schema = "CRYPTO_CASHBACK",
    full_refresh = false,
    post_hook = ["
                    {%
                    set final_table_relation = adapter.get_relation(
                            database=env_var('EXP_DATABASE', 'EXP'),
                            schema='PUBLIC',
                            identifier='API_CRYPTO_CASHBACK_MEMBER'
                        )
                    %}
                    {%
                    if final_table_relation is not none and '_AIRBYTE_UNIQUE_KEY' in adapter.get_columns_in_relation(final_table_relation)|map(attribute='name')
                    %}
                    -- Delete records which are no longer active
                    delete from {{ final_table_relation }} where {{ final_table_relation }}._AIRBYTE_UNIQUE_KEY in (
                        select recent_records.unique_key
                        from (
                                select distinct _AIRBYTE_UNIQUE_KEY as unique_key
                                from {{ this }}
                                where 1=1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', env_var('EXP_DATABASE', 'EXP') ~ '.PUBLIC.API_CRYPTO_CASHBACK_MEMBER') }}
                            ) recent_records
                            left join (
                                select _AIRBYTE_UNIQUE_KEY as unique_key, count(_AIRBYTE_UNIQUE_KEY) as active_count
                                from {{ this }}
                                where _AIRBYTE_ACTIVE_ROW = 1 {{ incremental_clause('_AIRBYTE_NORMALIZED_AT', env_var('EXP_DATABASE', 'EXP') ~ '.PUBLIC.API_CRYPTO_CASHBACK_MEMBER') }}
                                group by _AIRBYTE_UNIQUE_KEY
                            ) active_counts
                            on recent_records.unique_key = active_counts.unique_key
                        where active_count is null or active_count = 0
                    )
                    {% else %}
                    delete from {{ this }} where 1=0
                    {% endif %}
                    ",
                    "drop view {{ env_var('INTM_DATABASE', 'INTM') }}.CRYPTO_CASHBACK.API_CRYPTO_CASHBACK_MEMBER_STG"],
    tags = [ "top-level" ]
) }}
-- SCD Type 2 model for API_CRYPTO_CASHBACK_MEMBER
-- depends_on: {{ ref('API_CRYPTO_CASHBACK_MEMBER_STG') }}
with
{% if is_incremental() %}
new_data as (
    select *
    from {{ ref('API_CRYPTO_CASHBACK_MEMBER_STG')  }}
    where 1 = 1
    {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}
),
new_data_ids as (
    select distinct
        {{ dbt_utils.surrogate_key([
            'SITE_MEMBER_ID',
            'BRT_OFFER_ID'
        ]) }} as _AIRBYTE_UNIQUE_KEY
    from new_data
),
empty_new_data as (
    select * from new_data where 1 = 0
),
previous_active_scd_data as (
    select
        {{ star_intersect(ref('API_CRYPTO_CASHBACK_MEMBER_STG'), this, from_alias='inc_data', intersect_alias='this_data') }}
    from {{ this }} as this_data
    join new_data_ids on this_data._AIRBYTE_UNIQUE_KEY = new_data_ids._AIRBYTE_UNIQUE_KEY
    left join empty_new_data as inc_data on this_data._AIRBYTE_AB_ID = inc_data._AIRBYTE_AB_ID
    where _AIRBYTE_ACTIVE_ROW = 1
),
input_data as (
    select {{ dbt_utils.star(ref('API_CRYPTO_CASHBACK_MEMBER_STG')) }} from new_data
    union all
    select {{ dbt_utils.star(ref('API_CRYPTO_CASHBACK_MEMBER_STG')) }} from previous_active_scd_data
),
{% else %}
input_data as (
    select *
    from {{ ref('API_CRYPTO_CASHBACK_MEMBER_STG')  }}
),
{% endif %}
scd_data as (
    select
      {{ dbt_utils.surrogate_key([
      'SITE_MEMBER_ID',
      'BRT_OFFER_ID'
      ]) }} as _AIRBYTE_UNIQUE_KEY,
      SITE_MEMBER_ID,
      BRANDS,
      BRT_OFFER_ID,
      CLICK_DATE,
      SIGNUP_DATE,
      FTD_DATE,
      CPA_DATE,
      BASELINE_QUALIFIED,
      _AIRBYTE_EMITTED_AT as _AIRBYTE_START_AT,
      lag(_AIRBYTE_EMITTED_AT) over (
        partition by SITE_MEMBER_ID, BRT_OFFER_ID
        order by
            SITE_MEMBER_ID is null asc,
            SITE_MEMBER_ID desc,
            _AIRBYTE_EMITTED_AT desc
      ) as _AIRBYTE_END_AT,
      case when row_number() over (
        partition by SITE_MEMBER_ID, BRT_OFFER_ID
        order by
            SITE_MEMBER_ID is null asc,
            SITE_MEMBER_ID desc,
            _AIRBYTE_EMITTED_AT desc
      ) = 1 then 1 else 0 end as _AIRBYTE_ACTIVE_ROW,
      _AIRBYTE_AB_ID,
      _AIRBYTE_EMITTED_AT,
      _AIRBYTE_API_CRYPTO_CASHBACK_MEMBER_HASHID
    from input_data
),
dedup_data as (
    select
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
    SITE_MEMBER_ID,
    BRANDS,
    BRT_OFFER_ID,
    CLICK_DATE,
    SIGNUP_DATE,
    FTD_DATE,
    CPA_DATE,
    BASELINE_QUALIFIED,
    _AIRBYTE_START_AT,
    _AIRBYTE_END_AT,
    _AIRBYTE_ACTIVE_ROW,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_API_CRYPTO_CASHBACK_MEMBER_HASHID
from dedup_data where _AIRBYTE_ROW_NUM = 1
