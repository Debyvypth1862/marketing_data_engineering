{{ config(
    cluster_by = ["_AIRBYTE_EMITTED_AT"],
    unique_key = '_AIRBYTE_AB_ID',
    schema = "BRT",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('GOOGLE_ADS_CAMPAIGN_MARKETING_SITE_AB2') }}
select
    {{ dbt_utils.surrogate_key([
        'GOOGLE_ADS_CAMPAIGN_ID',
        'ID',
        'MARKETING_SITE_ID',
    ]) }} as _AIRBYTE_GOOGLE_ADS_CAMPAIGN_MARKETING_SITE_HASHID,
    tmp.*
from {{ ref('GOOGLE_ADS_CAMPAIGN_MARKETING_SITE_AB2') }} tmp
-- GOOGLE_ADS_CAMPAIGN_MARKETING_SITE
where 1 = 1

