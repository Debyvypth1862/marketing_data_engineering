{{ 
    config(
        materialized='view',
        database=env_var('STG_DATABASE'),
        schema="SPREE"
    ) 
}}
SELECT DISTINCT
    a.ID, 
    a.NAME, 
    a.MEMBERSHIPS,
    a._AIRBYTE_EMITTED_AT
FROM {{ source('VOLUUM', 'WORKSPACES') }} AS a
JOIN {{ ref('SPREE_VOLUUM_CAMPAIGNS') }} AS b
    ON a.id = b.workspace