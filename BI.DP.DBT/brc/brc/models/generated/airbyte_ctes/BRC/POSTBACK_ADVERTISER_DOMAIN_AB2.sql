{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
	try_cast(POAD_FK_ADVERTISER as {{ dbt_utils.type_float() }}) as POAD_FK_ADVERTISER,
	try_cast(POAD_FK_POSTBACK_DOMAIN as {{ dbt_utils.type_float() }}) as POAD_FK_POSTBACK_DOMAIN,
	try_cast(POAD_ID as {{ dbt_utils.type_float() }}) as POAD_ID,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('POSTBACK_ADVERTISER_DOMAIN_AB1') }}
-- POSTBACK_ADVERTISER_DOMAIN
where 1 = 1