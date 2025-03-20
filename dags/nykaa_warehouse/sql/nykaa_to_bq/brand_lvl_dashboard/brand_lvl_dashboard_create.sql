create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.brand_lvl_dashboard`
partition by mail_received_at
as

select
* except(rn),
from
(
  select
  brand_name,
  product_sku_distinct_count,
  demand_lvl_adr,
  demand_lvl_adr_value,
  inventory_qty,
  inventory_value_mrp,
  national_availability_per,
  open_po_qty,
  open_po_value,
  last_7_days_opp_loss_qty,
  opp_loss_daily_value_mrp,
  last_7_days_opp_loss_mrpper,
  cte_inv_qty_lt_6m,
  cte_inv_mrp_lt_6m,
  cte_per,
  excess_qty_over_3_month,
  excess_value_over_3_month,
  excess_qty_over_6_month,
  excess_value_over_6_month,

  RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
  DATE(pg_mail_recieved_at) AS mail_received_at,
  FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS report_day,
  row_number() over(partition by brand_name,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn,
  from `shopify-pubsub-project.pilgrim_bi_nykaa.brand_lvl_dashboard`
)
where rn = 1