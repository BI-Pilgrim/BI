create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.velocity_lvl_dashboard`
partition by mail_received_at
as

select
* except(rn),
from
(
  select
  coalesce(brand_name,'Pilgrim') as brand_name,
  coalesce(product_sku_distinct_count,0) as product_sku_distinct_count,
  coalesce(demand_lvl_adr,0) as demand_lvl_adr,
  coalesce(demand_lvl_adr_value,0) as demand_lvl_adr_value,
  coalesce(inventory_qty,0) as inventory_qty,
  coalesce(inventory_value_mrp,0) as inventory_value_mrp,
  coalesce(national_availability_per,0) as national_availability_per,
  coalesce(open_po_qty,0) as open_po_qty,
  coalesce(open_po_value,0) as open_po_value,
  coalesce(last_7_days_opp_loss_qty,0) as last_7_days_opp_loss_qty,
  coalesce(opp_loss_daily_value_mrp,0) as opp_loss_daily_value_mrp,
  coalesce(last_7_days_opp_loss_mrpper,0) as last_7_days_opp_loss_mrpper,
  coalesce(cte_inv_qty_lt_6m,0) as cte_inv_qty_lt_6m,
  coalesce(cte_inv_mrp_lt_6m,0) as cte_inv_mrp_lt_6m,
  coalesce(cte_per,0) as cte_per,
  coalesce(excess_qty_over_3_month,0) as excess_qty_over_3_month,
  coalesce(excess_value_over_3_month,0) as excess_value_over_3_month,
  coalesce(excess_qty_over_6_month,0) as excess_qty_over_6_month,
  coalesce(excess_value_over_6_month,0) as excess_value_over_6_month,
  case
    when brand_name = 'Pilgrim Total' then 'Pilgrim Total'
    when brand_name = 'Grand Total' then 'Grand Total'
    else sku_velocity
  end as sku_velocity,

  RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
  DATE(pg_mail_recieved_at) AS mail_received_at,
  FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS report_day,
  row_number() over(partition by brand_name,sku_velocity,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn,
  from `shopify-pubsub-project.pilgrim_bi_nykaa.velocity_lvl_dashboard`
)
where rn = 1