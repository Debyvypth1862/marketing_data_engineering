{{
    config(
        materialized='incremental',
        unique_key='hash_key',
        cluster_by=['sale_date', 'hash_key']
    )
}}

-- Transform Play55 sales data from Portuguese to English column names
-- Source: STG.APOSTA_PREMIA.PLAY55_VENDAS -> Target: RAW.APOSTA_PREMIA.PLAY55_VENDAS
--
-- Incremental strategy: Uses Airbyte's _AIRBYTE_EXTRACTED_AT to process only newly synced records.
-- This ensures backfilled data (with old sale_date but recent extraction time) is not skipped.

with source_data as (
    select
        DATA,
        OPERACAO_CODIGO,
        NSU,
        SORTEIO_ID,
        SORTEIO_DESCRICAO,
        COMPRADOR_NOME,
        COMPRADOR_EMAIL,
        COMPRADOR_TELEFONE,
        COMPRA_BRINDE,
        AFILIADO_CODIGO,
        DATA_VENDA,
        VALOR_TOTAL,
        QTD_TOTAL,
        UTM_SOURCE,
        UTM_MEDIUM,
        UTM_CAMPAIGN,
        UTM_TERM,
        UTM_CONTENT,
        DEPARA_SOURCE,
        OPERADOR_ID,
        PERCENTUAL_COMISSAO,
        COMISSAO,
        DEPARA_MEDIUM,
        HASH_KEY,
        _AIRBYTE_EXTRACTED_AT
    from {{ source('APOSTA_PREMIA', 'PLAY55_VENDAS') }}

    {% if is_incremental() %}
    -- Only process records extracted by Airbyte since last dbt run
    -- This ensures backfilled data (old sale dates) gets processed
    where _AIRBYTE_EXTRACTED_AT > (select coalesce(max(airbyte_extracted_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
)

select
    -- Column mapping: Portuguese/Mixed → English
    DATA as date,
    OPERACAO_CODIGO as operation_code,
    NSU as nsu,
    SORTEIO_ID as raffle_id,
    SORTEIO_DESCRICAO as raffle_description,
    COMPRADOR_NOME as buyer_name,
    COMPRADOR_EMAIL as buyer_email,
    COMPRADOR_TELEFONE as buyer_phone,
    COMPRA_BRINDE as gift_purchase,
    AFILIADO_CODIGO as affiliate_code,
    DATA_VENDA as sale_date,
    VALOR_TOTAL as total_amount,
    QTD_TOTAL as total_quantity,
    UTM_SOURCE as utm_source,
    UTM_MEDIUM as utm_medium,
    UTM_CAMPAIGN as utm_campaign,
    UTM_TERM as utm_term,
    UTM_CONTENT as utm_content,
    DEPARA_SOURCE as depara_source,
    OPERADOR_ID as operator_id,
    PERCENTUAL_COMISSAO as commission_percentage,
    COMISSAO as commission_amount,
    DEPARA_MEDIUM as depara_medium,
    HASH_KEY as hash_key,
    _AIRBYTE_EXTRACTED_AT as airbyte_extracted_at
from source_data
