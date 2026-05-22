{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('GOOGLE_ADS_CAMPAIGNS_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'NOTES',
        'BUDGET_TIMING',
        'CAMPAIGN_TYPE',
        'CREATED_AT',
        'CREATED_BY',
        'DELETED_AT',
        'CURRENCY_CODE',
        'GOOGLE_ADS_ACCOUNT_ID',
        'UPDATED_AT',
        'NAME',
        'UPDATED_BY',
        'ID',
        'CAMPAIGN_ID',
        'DATA_COLLECTION_METHOD',
        boolean_to_string('STATUS'),
    ]) }} as _AIRBYTE_GOOGLE_ADS_CAMPAIGNS_HASHID,
    tmp.*
from {{ ref('GOOGLE_ADS_CAMPAIGNS_AB2') }} tmp
-- GOOGLE_ADS_CAMPAIGNS
where 1 = 1

