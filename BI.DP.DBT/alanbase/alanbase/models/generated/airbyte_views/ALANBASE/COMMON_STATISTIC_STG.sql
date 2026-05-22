{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    database = env_var('STG_DATABASE'),
    schema = "ALANBASE",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('COMMON_STATISTIC_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'CLICKID',
        'DATE',
        'TRACKER_LOGIN_ID'
    ]) }} as _AIRBYTE_COMMON_STATISTIC_HASHID,
    tmp.*
from {{ ref('COMMON_STATISTIC_AB2') }} tmp
-- COMMON_STATISTIC
where 1 = 1
{{ incremental_clause('_AIRBYTE_EMITTED_AT', this) }}

