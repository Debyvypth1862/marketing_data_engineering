{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	cast(POST_SUBID4 as {{ dbt_utils.type_string() }}) as POST_SUBID4,
    case
        when POST_FTD_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}' then to_timestamp(POST_FTD_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS')
        when POST_FTD_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}' then to_timestamp(POST_FTD_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS.FF')
        when POST_FTD_TIMESTAMP = '' then NULL
    else to_timestamp(POST_FTD_TIMESTAMP)
    end as POST_FTD_TIMESTAMP
    ,
    cast(POST_FBCLID as {{ dbt_utils.type_string() }}) as POST_FBCLID,
    cast(POST_SUBID5 as {{ dbt_utils.type_string() }}) as POST_SUBID5,
    cast(POST_OW_ID as {{ dbt_utils.type_string() }}) as POST_OW_ID,
    cast(POST_AFFILIATE_ID as {{ dbt_utils.type_string() }}) as POST_AFFILIATE_ID,
    cast(POST_IP as {{ dbt_utils.type_string() }}) as POST_IP,
    cast({{ empty_string_to_null('POST_CLICK_DATE') }} as {{ type_date() }}) as POST_CLICK_DATE,
    cast(POST_PAGE_LOCATION as {{ dbt_utils.type_string() }}) as POST_PAGE_LOCATION,
    cast(POST_FK_TRACKER as {{ dbt_utils.type_bigint() }}) as POST_FK_TRACKER,
    case
        when POST_CLICK_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(POST_CLICK_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when POST_CLICK_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(POST_CLICK_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when POST_CLICK_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(POST_CLICK_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when POST_CLICK_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(POST_CLICK_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when POST_CLICK_TIMESTAMP = '' then NULL
    else to_timestamp_tz(POST_CLICK_TIMESTAMP)
    end as POST_CLICK_TIMESTAMP
    ,
    cast(POST_SUBID2 as {{ dbt_utils.type_string() }}) as POST_SUBID2,
    cast(POST_FK_CAMT_ID as {{ dbt_utils.type_bigint() }}) as POST_FK_CAMT_ID,
    cast(POST_SUBID3 as {{ dbt_utils.type_string() }}) as POST_SUBID3,
    cast(POST_ADGROUPID as {{ dbt_utils.type_string() }}) as POST_ADGROUPID,
    cast(POST_UTM_CONTENT as {{ dbt_utils.type_string() }}) as POST_UTM_CONTENT,
    cast(POST_UTM_ID as {{ dbt_utils.type_string() }}) as POST_UTM_ID,
    cast(POST_SUBID as {{ dbt_utils.type_string() }}) as POST_SUBID,
    cast(POST_GA4_DEVICE_ID as {{ dbt_utils.type_string() }}) as POST_GA4_DEVICE_ID,
    case
        when POST_MODIFIED_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{4}' then to_timestamp_tz(POST_MODIFIED_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SSTZHTZM')
        when POST_MODIFIED_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}(\\+|-)\\d{2}' then to_timestamp_tz(POST_MODIFIED_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SSTZH')
        when POST_MODIFIED_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{4}' then to_timestamp_tz(POST_MODIFIED_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS.FFTZHTZM')
        when POST_MODIFIED_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}(\\+|-)\\d{2}' then to_timestamp_tz(POST_MODIFIED_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS.FFTZH')
        when POST_MODIFIED_TIMESTAMP = '' then NULL
    else to_timestamp_tz(POST_MODIFIED_TIMESTAMP)
    end as POST_MODIFIED_TIMESTAMP
    ,
    case
        when POST_CPA_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}' then to_timestamp(POST_CPA_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS')
        when POST_CPA_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}' then to_timestamp(POST_CPA_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS.FF')
        when POST_CPA_TIMESTAMP = '' then NULL
    else to_timestamp(POST_CPA_TIMESTAMP)
    end as POST_CPA_TIMESTAMP
    ,
    cast(POST_UTM_SOURCE as {{ dbt_utils.type_string() }}) as POST_UTM_SOURCE,
    cast({{ empty_string_to_null('POST_SIGNUP_DATE') }} as {{ type_date() }}) as POST_SIGNUP_DATE,
    cast(POST_SITE_MEMBER_ID as {{ dbt_utils.type_string() }}) as POST_SITE_MEMBER_ID,
    cast(POST_UTM_CAMPAIGN as {{ dbt_utils.type_string() }}) as POST_UTM_CAMPAIGN,
    cast(POST_CAMPAIGNID as {{ dbt_utils.type_string() }}) as POST_CAMPAIGNID,
    cast(POST_UTM_MEDIUM as {{ dbt_utils.type_string() }}) as POST_UTM_MEDIUM,
    cast(POST_ENV as {{ dbt_utils.type_string() }}) as POST_ENV,
    cast(POST_KEYWORD as {{ dbt_utils.type_string() }}) as POST_KEYWORD,
    cast(POST_CREATIVE as {{ dbt_utils.type_string() }}) as POST_CREATIVE,
    cast({{ empty_string_to_null('POST_FTD_DATE') }} as {{ type_date() }}) as POST_FTD_DATE,
    cast(POST_PAGE as {{ dbt_utils.type_string() }}) as POST_PAGE,
    cast(POST_GCLID as {{ dbt_utils.type_string() }}) as POST_GCLID,
    case
        when POST_SIGNUP_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}' then to_timestamp(POST_SIGNUP_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS')
        when POST_SIGNUP_TIMESTAMP regexp '\\d{4}-\\d{2}-\\d{2}T(\\d{2}:){2}\\d{2}\\.\\d{1,7}' then to_timestamp(POST_SIGNUP_TIMESTAMP, 'YYYY-MM-DDTHH24:MI:SS.FF')
        when POST_SIGNUP_TIMESTAMP = '' then NULL
    else to_timestamp(POST_SIGNUP_TIMESTAMP)
    end as POST_SIGNUP_TIMESTAMP
    ,
    cast(POST_UTM_TERM as {{ dbt_utils.type_string() }}) as POST_UTM_TERM,
    cast(POST_3RD_PARTY_CLICKID as {{ dbt_utils.type_string() }}) as POST_3RD_PARTY_CLICKID,
    cast(POST_ID as {{ dbt_utils.type_bigint() }}) as POST_ID,
    cast(POST_ADACCOUNTID as {{ dbt_utils.type_string() }}) as POST_ADACCOUNTID,
    cast(POST_MARKETING_SITE_ID as {{ dbt_utils.type_string() }}) as POST_MARKETING_SITE_ID,
    cast(POST_CLICKID as {{ dbt_utils.type_string() }}) as POST_CLICKID,
    cast({{ empty_string_to_null('POST_CPA_DATE') }} as {{ type_date() }}) as POST_CPA_DATE,
    cast(POST_TEST_VARIATION as {{ dbt_utils.type_string() }}) as POST_TEST_VARIATION,
    cast(POST_APP_INSTANCE_ID as {{ dbt_utils.type_string() }}) as POST_APP_INSTANCE_ID,
    cast(POST_FIREBASE_APP_ID as {{ dbt_utils.type_string() }}) as POST_FIREBASE_APP_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	S3_PATH,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_TRACKING_AB1') }}
-- POSTBACK_TRACKING
where 1 = 1
-- {{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}