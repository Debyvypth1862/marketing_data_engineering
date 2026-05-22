{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    database = env_var('RAW_DATABASE'),
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='OPERATOR_ACCOUNT_VARIABLES_STG'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('OPERATOR_ACCOUNT_VARIABLES_AB3') }}
select
    ID,
    VALUE,
    KEY,
    OPERATOR_ACCOUNT_ID,
    _AIRBYTE_AB_ID,
    _AIRBYTE_EMITTED_AT,
    {{ current_timestamp() }} as _AIRBYTE_NORMALIZED_AT,
    _AIRBYTE_OPERATOR_ACCOUNT_VARIABLES_HASHID
from {{ ref('OPERATOR_ACCOUNT_VARIABLES_AB3') }}
-- OPERATOR_ACCOUNT_VARIABLES from {{ source('BRT', '_AIRBYTE_RAW_OPERATOR_ACCOUNT_VARIABLES') }}
where 1 = 1

