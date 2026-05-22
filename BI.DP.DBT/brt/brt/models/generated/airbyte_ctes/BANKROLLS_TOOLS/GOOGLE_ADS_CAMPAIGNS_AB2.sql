{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('GOOGLE_ADS_CAMPAIGNS_AB1') }}
select
    cast(NOTES as {{ dbt_utils.type_string() }}) as NOTES,
    cast(BUDGET_TIMING as {{ dbt_utils.type_string() }}) as BUDGET_TIMING,
    cast(CAMPAIGN_TYPE as {{ dbt_utils.type_string() }}) as CAMPAIGN_TYPE,
    case
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when CREATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(CREATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when CREATED_AT = '' then NULL
    else to_timestamp_tz(CREATED_AT)
    end as CREATED_AT
    ,
    cast(CREATED_BY as {{ dbt_utils.type_bigint() }}) as CREATED_BY,
    case
        when DELETED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(DELETED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when DELETED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(DELETED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when DELETED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(DELETED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when DELETED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(DELETED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when DELETED_AT = '' then NULL
    else to_timestamp_tz(DELETED_AT)
    end as DELETED_AT
    ,
    cast(CURRENCY_CODE as {{ dbt_utils.type_string() }}) as CURRENCY_CODE,
    cast(GOOGLE_ADS_ACCOUNT_ID as {{ dbt_utils.type_bigint() }}) as GOOGLE_ADS_ACCOUNT_ID,
    case
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when UPDATED_AT regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(UPDATED_AT, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when UPDATED_AT = '' then NULL
    else to_timestamp_tz(UPDATED_AT)
    end as UPDATED_AT
    ,
    cast(NAME as {{ dbt_utils.type_string() }}) as NAME,
    cast(UPDATED_BY as {{ dbt_utils.type_bigint() }}) as UPDATED_BY,
    cast(ID as {{ dbt_utils.type_bigint() }}) as ID,
    cast(CAMPAIGN_ID as {{ dbt_utils.type_string() }}) as CAMPAIGN_ID,
    cast(DATA_COLLECTION_METHOD as {{ dbt_utils.type_bigint() }}) as DATA_COLLECTION_METHOD,
    {{ cast_to_boolean('STATUS') }} as STATUS,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('GOOGLE_ADS_CAMPAIGNS_AB1') }}
-- GOOGLE_ADS_CAMPAIGNS
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

