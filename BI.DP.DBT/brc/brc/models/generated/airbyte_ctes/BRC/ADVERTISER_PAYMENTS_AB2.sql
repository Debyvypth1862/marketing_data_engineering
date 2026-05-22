{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(ADPA_CREATED as {{ dbt_utils.type_string() }}) as ADPA_CREATED,
	try_cast(ADPA_ID as {{ dbt_utils.type_float() }}) as ADPA_ID,
	try_cast(ADPA_MONTH as {{ dbt_utils.type_string() }}) as ADPA_MONTH,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('ADVERTISER_PAYMENTS_AB1') }}
-- ADVERTISER_PAYMENTS
where 1 = 1
AND _AIRBYTE_EMITTED_AT = 
(SELECT MAX(_AIRBYTE_EMITTED_AT) FROM {{ source('BRC', '_AIRBYTE_RAW_ADMINS') }})