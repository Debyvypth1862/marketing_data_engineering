{{ config(
    cluster_by = ["_AIRBYTE_UNIQUE_KEY", "CREATED_AT"],
    unique_key = "_AIRBYTE_UNIQUE_KEY",
    incremental_strategy = "delete+insert",
    database = env_var('EXP_DATABASE', 'EXP'),
    schema = "PUBLIC",
    tags = [ "top-level" ]
) }}
-- Final table for API_CRYPTO_CASHBACK_MEMBER with created_at/modified_at
-- depends_on: {{ ref('API_CRYPTO_CASHBACK_MEMBER_SCD') }}

with scd_active as (
    select *
    from {{ ref('API_CRYPTO_CASHBACK_MEMBER_SCD') }}
    where _AIRBYTE_ACTIVE_ROW = 1
    {% if is_incremental() %}
        and _AIRBYTE_EMITTED_AT > (select max(CREATED_AT) from {{ this }})
    {% endif %}
),

first_version as (
    select
        _AIRBYTE_UNIQUE_KEY,
        min(_AIRBYTE_START_AT) as first_start_at,
        min_by(_AIRBYTE_API_CRYPTO_CASHBACK_MEMBER_HASHID, _AIRBYTE_START_AT) as first_hash
    from {{ ref('API_CRYPTO_CASHBACK_MEMBER_SCD') }}
    group by _AIRBYTE_UNIQUE_KEY
)

select
    scd._AIRBYTE_UNIQUE_KEY,
    scd.SITE_MEMBER_ID,
    scd.BRANDS,
    scd.BRT_OFFER_ID,
    scd.CLICK_DATE,
    scd.SIGNUP_DATE,
    scd.FTD_DATE,
    scd.CPA_DATE,
    scd.BASELINE_QUALIFIED,
    scd._AIRBYTE_AB_ID,
    scd._AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    scd._AIRBYTE_API_CRYPTO_CASHBACK_MEMBER_HASHID,
    fv.first_start_at as CREATED_AT,
    case
        when scd._AIRBYTE_API_CRYPTO_CASHBACK_MEMBER_HASHID = fv.first_hash then null
        else scd._AIRBYTE_START_AT
    end as MODIFIED_AT
from scd_active scd
left join first_version fv
    on scd._AIRBYTE_UNIQUE_KEY = fv._AIRBYTE_UNIQUE_KEY
