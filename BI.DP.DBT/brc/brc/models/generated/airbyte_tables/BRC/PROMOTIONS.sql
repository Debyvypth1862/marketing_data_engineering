{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = "_AIRBYTE_AB_ID",
	database = env_var('RAW_DATABASE'),
    schema = "BRC",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('PROMOTIONS_AB2') }}
select
	PROM_BANNER,
	PROM_FK_ADVERTISERS,
	PROM_ID,
	PROM_MAIN_COMP,
	PROM_MONTH,
	PROM_NAME,
	PROM_POSITION,
	PROM_TEXT,
	_AIRBYTE_AB_ID,
	_AIRBYTE_EMITTED_AT,
	{{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT
from {{ ref('PROMOTIONS_AB2') }}
-- PROMOTIONS from {{ source('BRC', '_AIRBYTE_RAW_PROMOTIONS') }}
where 1 = 1