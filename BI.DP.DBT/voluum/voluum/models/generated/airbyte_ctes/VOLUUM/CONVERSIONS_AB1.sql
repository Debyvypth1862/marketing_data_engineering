{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "VOLUUM",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('VOLUUM', '_AIRBYTE_RAW_CONVERSIONS') }}
WITH s3_status AS (
    SELECT PATH, IS_PROCESSED, PICKED_FOR_REPROCESS
    FROM {{ source('PUBLIC', 'S3_FILES_STATS') }} 
)
select
    {{ json_extract_scalar('_airbyte_data', ['allConversionsRevenue'], ['allConversionsRevenue']) }} as ALL_CONVERSIONS_REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['affiliateNetworkId'], ['affiliateNetworkId']) }} as AFFILIATE_NETWORK_ID,
    {{ json_extract_scalar('_airbyte_data', ['affiliateNetworkName'], ['affiliateNetworkName']) }} as AFFILIATE_NETWORK_NAME,
    {{ json_extract_scalar('_airbyte_data', ['brand'], ['brand']) }} as BRAND,
    {{ json_extract_scalar('_airbyte_data', ['browser'], ['browser']) }} as BROWSER,
    {{ json_extract_scalar('_airbyte_data', ['browserVersion'], ['browserVersion']) }} as BROWSER_VERSION,
    {{ json_extract_scalar('_airbyte_data', ['campaignId'], ['campaignId']) }} as CAMPAIGN_ID,
    {{ json_extract_scalar('_airbyte_data', ['campaignName'], ['campaignName']) }} as CAMPAIGN_NAME,
    {{ json_extract_scalar('_airbyte_data', ['campaignUrlConfigured'], ['campaignUrlConfigured']) }} as CAMPAIGN_URL_CONFIGURED,
    {{ json_extract_scalar('_airbyte_data', ['city'], ['city']) }} as CITY,
    {{ json_extract_scalar('_airbyte_data', ['clickId'], ['clickId']) }} as CLICKID,
    {{ json_extract_scalar('_airbyte_data', ['connectionType'], ['connectionType']) }} as CONNECTION_TYPE,
    {{ json_extract_scalar('_airbyte_data', ['connectionTypeName'], ['connectionTypeName']) }} as CONNECTION_TYPE_NAME,
    {{ json_extract_scalar('_airbyte_data', ['conversionOriginalCurrency'], ['conversionOriginalCurrency']) }} as CONVERSION_ORIGINAL_CURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['conversionType'], ['conversionType']) }} as CONVERSION_TYPE,
    {{ json_extract_scalar('_airbyte_data', ['conversionTypeId'], ['conversionTypeId']) }} as CONVERSION_TYPE_ID,
    {{ json_extract_scalar('_airbyte_data', ['cost'], ['cost']) }} as COST,
    {{ json_extract_scalar('_airbyte_data', ['costSources'], ['costSources']) }} as COST_SOURCES,
    {{ json_extract_scalar('_airbyte_data', ['countryCode'], ['countryCode']) }} as COUNTRY_CODE,
    {{ json_extract_scalar('_airbyte_data', ['countryName'], ['countryName']) }} as COUNTRY_NAME,
    {{ json_extract_scalar('_airbyte_data', ['customVariable1'], ['customVariable1']) }} as CUSTOM_VARIABLE_1,
    {{ json_extract_scalar('_airbyte_data', ['customVariable10'], ['customVariable10']) }} as CUSTOM_VARIABLE_10,
    {{ json_extract_scalar('_airbyte_data', ['customVariable2'], ['customVariable2']) }} as CUSTOM_VARIABLE_2,
    {{ json_extract_scalar('_airbyte_data', ['customVariable3'], ['customVariable3']) }} as CUSTOM_VARIABLE_3,
    {{ json_extract_scalar('_airbyte_data', ['customVariable4'], ['customVariable4']) }} as CUSTOM_VARIABLE_4,
    {{ json_extract_scalar('_airbyte_data', ['customVariable5'], ['customVariable5']) }} as CUSTOM_VARIABLE_5,
    {{ json_extract_scalar('_airbyte_data', ['customVariable6'], ['customVariable6']) }} as CUSTOM_VARIABLE_6,
    {{ json_extract_scalar('_airbyte_data', ['customVariable7'], ['customVariable7']) }} as CUSTOM_VARIABLE_7,
    {{ json_extract_scalar('_airbyte_data', ['customVariable8'], ['customVariable8']) }} as CUSTOM_VARIABLE_8,
    {{ json_extract_scalar('_airbyte_data', ['customVariable9'], ['customVariable9']) }} as CUSTOM_VARIABLE_9,
    {{ json_extract_scalar('_airbyte_data', ['device'], ['device']) }} as DEVICE,
    {{ json_extract_scalar('_airbyte_data', ['deviceName'], ['deviceName']) }} as DEVICE_NAME,
    {{ json_extract_scalar('_airbyte_data', ['externalId'], ['externalId']) }} as EXTERNAL_ID,
    {{ json_extract_scalar('_airbyte_data', ['flowId'], ['flowId']) }} as FLOW_ID,
    {{ json_extract_scalar('_airbyte_data', ['ip'], ['ip']) }} as IP,
    {{ json_extract_scalar('_airbyte_data', ['isDsp'], ['isDsp']) }} as IS_DSP,
    {{ json_extract_scalar('_airbyte_data', ['isp'], ['isp']) }} as ISP,
    {{ json_extract_scalar('_airbyte_data', ['landerId'], ['landerId']) }} as LANDER_ID,
    {{ json_extract_scalar('_airbyte_data', ['landerName'], ['landerName']) }} as LANDER_NAME,
    {{ json_extract_scalar('_airbyte_data', ['mobileCarrier'], ['mobileCarrier']) }} as MOBILE_CARRIER,
    {{ json_extract_scalar('_airbyte_data', ['model'], ['model']) }} as MODEL,
    {{ json_extract_scalar('_airbyte_data', ['offerId'], ['offerId']) }} as OFFER_ID,
    {{ json_extract_scalar('_airbyte_data', ['offerName'], ['offerName']) }} as OFFER_NAME,
    {{ json_extract_scalar('_airbyte_data', ['os'], ['os']) }} as OS,
    {{ json_extract_scalar('_airbyte_data', ['osVersion'], ['osVersion']) }} as OS_VERSION,
    {{ json_extract_scalar('_airbyte_data', ['outgoingPostbackUrl'], ['outgoingPostbackUrl']) }} as OUTGOING_POSTBACK_URL,
    {{ json_extract_scalar('_airbyte_data', ['pathId'], ['pathId']) }} as PATH_ID,
    {{ json_extract_scalar('_airbyte_data', ['postbackParam1'], ['postbackParam1']) }} as POSTBACK_PARAM_1,
    {{ json_extract_scalar('_airbyte_data', ['postbackParam2'], ['postbackParam2']) }} as POSTBACK_PARAM_2,
    {{ json_extract_scalar('_airbyte_data', ['postbackParam3'], ['postbackParam3']) }} as POSTBACK_PARAM_3,
    {{ json_extract_scalar('_airbyte_data', ['postbackParam4'], ['postbackParam4']) }} as POSTBACK_PARAM_4,
    {{ json_extract_scalar('_airbyte_data', ['postbackParam5'], ['postbackParam5']) }} as POSTBACK_PARAM_5,
    {{ json_extract_scalar('_airbyte_data', ['postbackTimestamp'], ['postbackTimestamp']) }} as POSTBACK_TIMESTAMP,
    {{ json_extract_scalar('_airbyte_data', ['referrer'], ['referrer']) }} as REFERRER,
    {{ json_extract_scalar('_airbyte_data', ['region'], ['region']) }} as REGION,
    {{ json_extract_scalar('_airbyte_data', ['revenue'], ['revenue']) }} as REVENUE,
    {{ json_extract_scalar('_airbyte_data', ['revenueInOriginalCurrency'], ['revenueInOriginalCurrency']) }} as REVENUE_IN_ORIGINAL_CURRENCY,
    {{ json_extract_scalar('_airbyte_data', ['subLanderId'], ['subLanderId']) }} as SUB_LANDER_ID,
    {{ json_extract_scalar('_airbyte_data', ['trafficSourceId'], ['trafficSourceId']) }} as TRAFFIC_SOURCE_ID,
    {{ json_extract_scalar('_airbyte_data', ['trafficSourceName'], ['trafficSourceName']) }} as TRAFFIC_SOURCE_NAME,
    {{ json_extract_scalar('_airbyte_data', ['transactionId'], ['transactionId']) }} as TRANSACTION_ID,
    {{ json_extract_scalar('_airbyte_data', ['type'], ['type']) }} as TYPE,
    {{ json_extract_scalar('_airbyte_data', ['userAgent'], ['userAgent']) }} as USER_AGENT,
    {{ json_extract_scalar('_airbyte_data', ['visitTimestamp '], ['visitTimestamp ']) }} as VISIT_TIMESTAMP,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    S3_PATH,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ source('VOLUUM', '_AIRBYTE_RAW_CONVERSIONS') }} as table_alias
JOIN s3_status s3
    ON table_alias.S3_PATH = s3.PATH

WHERE s3.IS_PROCESSED = FALSE AND s3.PICKED_FOR_REPROCESS = FALSE
AND table_alias._AIRBYTE_EMITTED_AT >= CURRENT_DATE

