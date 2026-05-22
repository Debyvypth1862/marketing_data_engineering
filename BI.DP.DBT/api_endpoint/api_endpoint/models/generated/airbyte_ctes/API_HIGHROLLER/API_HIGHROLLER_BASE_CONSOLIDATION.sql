{{ config(
    materialized = "ephemeral"
) }}
-- Base Consolidation for highroller - Based on V_HIGHROLLER_API view
select 
    ops.date,
    ops.signup_date,
    ops.ftd_date,
    ops.country,
    ops.publisher_name,
    ops.advertiser_id,
    ops.advertiser_name,
    a.camp_name,
    ops.brand_name,
    ops.clickid,
    ops.click_cnt,
    ops.signup_cnt,
    ops.ftd_cnt,
    IFNULL(ops.deposit_amt,0) AS deposit_amt,
    IFNULL(ops.ftd_amt,0) as ftd_amt,
    IFNULL(ops.net_revenue_amt,0) as net_revenue_amt
from {{ source('EXP_PUBLIC', 'FACT_OPERATOR_AGG') }} ops
left outer join {{ source('BRC', 'POSTBACK_TRACKING') }} pstbk
    on upper(ops.clickid) = upper(pstbk.post_clickid)
left outer join {{ source('BRC', 'CAMPAIGN_TRACKERS') }} cmtkr 
    on pstbk.POST_FK_CAMT_ID = cmtkr.CAMT_ID
left outer join {{ source('BRC', 'CAMPAIGNS') }} a 
    on cmtkr.CAMT_FK_CAMPAIGN = a.CAMP_ID
where 
    upper(ops.advertiser_name) like '%HIGH%'
    and ops.country = 'Canada'