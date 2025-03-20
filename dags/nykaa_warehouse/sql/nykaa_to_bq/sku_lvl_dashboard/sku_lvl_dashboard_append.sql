merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_lvl_dashboard` as target
using
(
select
* except(rn),
from
(
  select
  sku_status,
  brand_name,
  ean_code,
  sku_desc,
  last_grndate,
  pack_size,
  mrp,
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
  coalesce(sku_code,'-') as sku_code,
  coalesce(sku_velocity,'-') as sku_velocity,

  RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
  DATE(pg_mail_recieved_at) AS mail_received_at,
  FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS report_day,
  row_number() over(partition by sku_status,ean_code,last_grndate,pack_size,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn,
  from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_lvl_dashboard`
)
where rn = 1 and mail_received_at >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source

on target.brand_name = source.brand_name
and target.sku_velocity = source.sku_velocity
and target.reporting_week = source.reporting_week

when matched and target.mail_received_at < source.mail_received_at
then update set

target.sku_status = source.sku_status,
target.brand_name = source.brand_name,
target.ean_code = source.ean_code,
target.sku_desc = source.sku_desc,
target.last_grndate = source.last_grndate,
target.pack_size = source.pack_size,
target.mrp = source.mrp,
target.demand_lvl_adr = source.demand_lvl_adr,
target.demand_lvl_adr_value = source.demand_lvl_adr_value,
target.inventory_qty = source.inventory_qty,
target.inventory_value_mrp = source.inventory_value_mrp,
target.national_availability_per = source.national_availability_per,
target.open_po_qty = source.open_po_qty,
target.open_po_value = source.open_po_value,
target.last_7_days_opp_loss_qty = source.last_7_days_opp_loss_qty,
target.opp_loss_daily_value_mrp = source.opp_loss_daily_value_mrp,
target.last_7_days_opp_loss_mrpper = source.last_7_days_opp_loss_mrpper,
target.cte_inv_qty_lt_6m = source.cte_inv_qty_lt_6m,
target.cte_inv_mrp_lt_6m = source.cte_inv_mrp_lt_6m,
target.cte_per = source.cte_per,
target.excess_qty_over_3_month = source.excess_qty_over_3_month,
target.excess_value_over_3_month = source.excess_value_over_3_month,
target.excess_qty_over_6_month = source.excess_qty_over_6_month,
target.excess_value_over_6_month = source.excess_value_over_6_month,
target.sku_code = source.sku_code,
target.sku_velocity = source.sku_velocity,
target.reporting_week = source.reporting_week,
target.mail_received_at = source.mail_received_at,
target.report_day = source.report_day

when not matched
then insert
(
sku_status,
brand_name,
ean_code,
sku_desc,
last_grndate,
pack_size,
mrp,
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
sku_code,
sku_velocity,
reporting_week,
mail_received_at,
report_day
)
values
(
source.sku_status,
source.brand_name,
source.ean_code,
source.sku_desc,
source.last_grndate,
source.pack_size,
source.mrp,
source.demand_lvl_adr,
source.demand_lvl_adr_value,
source.inventory_qty,
source.inventory_value_mrp,
source.national_availability_per,
source.open_po_qty,
source.open_po_value,
source.last_7_days_opp_loss_qty,
source.opp_loss_daily_value_mrp,
source.last_7_days_opp_loss_mrpper,
source.cte_inv_qty_lt_6m,
source.cte_inv_mrp_lt_6m,
source.cte_per,
source.excess_qty_over_3_month,
source.excess_value_over_3_month,
source.excess_qty_over_6_month,
source.excess_value_over_6_month,
source.sku_code,
source.sku_velocity,
source.reporting_week,
source.mail_received_at,
source.report_day
)