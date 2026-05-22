{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
	database = env_var('STG_DATABASE'),
    schema = "BRC",
    tags = [ "top-level-intermediate" ]
) }}
select
    {{ dbt_utils.surrogate_key([
		'POST_3RD_PARTY_CLICKID',
		'POST_CLICK_HTTPCODE',
		'POST_CLICK_TIMESTAMP',
		'POST_CLICK_URL',
		'POST_CLICKID',
		'POST_FK_CAMT_ID',
		'POST_FK_TRACKER',
		'POST_ID',
		'POST_SUBID',
		'POST_SUBID_2',
		'POST_SUBID_3',
		'POST_SUBID_4',
		'POST_SUBID_5',
	]) }} as _AIRBYTE_POSTBACK_3RD_PARTY_CLICK_LOG_HASHID,
	tmp.*
from {{ ref('POSTBACK_3RD_PARTY_CLICK_LOG_AB2') }} tmp
-- POSTBACK_3RD_PARTY_CLICK_LOG
where 1 = 1 
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}