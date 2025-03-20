merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.open_rtv` as target
using
(
select
* except(rn)
from
(
  select
  rtv_no,
  product_sku,
  desctiption,
  brand_name,
  rtv_status,
  reason,
  location_name,
  rtv_type,
  rtv_ageing,
  vendor_code,
  awb,
  rtv_qty,
  rtv_value,
  RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
  DATE(pg_mail_recieved_at) AS mail_received_at,
  FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS day_name,
  row_number() over(partition by rtv_no,product_sku,rtv_status,reason,location_name,rtv_type,rtv_ageing,vendor_code,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn
  from `shopify-pubsub-project.pilgrim_bi_nykaa.open_rtv`
  -- where rtv_no = 'BL26367'
  -- and product_sku = 'PILGR00000146'
)
where rn = 1 and mail_received_at >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source

on target.brand_name = source.brand_name
and target.reporting_week = source.reporting_week

when matched and target.mail_received_at < source.mail_received_at
then update set

target.rtv_no = source.rtv_no,
target.product_sku = source.product_sku,
target.desctiption = source.desctiption,
target.brand_name = source.brand_name,
target.rtv_status = source.rtv_status,
target.reason = source.reason,
target.location_name = source.location_name,
target.rtv_type = source.rtv_type,
target.rtv_ageing = source.rtv_ageing,
target.vendor_code = source.vendor_code,
target.awb = source.awb,
target.rtv_qty = source.rtv_qty,
target.rtv_value = source.rtv_value,
target.reporting_week = source.reporting_week,
target.mail_received_at = source.mail_received_at,
target.day_name = source.day_name


when not matched
then insert
(
rtv_no,
product_sku,
desctiption,
brand_name,
rtv_status,
reason,
location_name,
rtv_type,
rtv_ageing,
vendor_code,
awb,
rtv_qty,
rtv_value,
reporting_week,
mail_received_at,
day_name
)
values
(
source.rtv_no,
source.product_sku,
source.desctiption,
source.brand_name,
source.rtv_status,
source.reason,
source.location_name,
source.rtv_type,
source.rtv_ageing,
source.vendor_code,
source.awb,
source.rtv_qty,
source.rtv_value,
source.reporting_week,
source.mail_received_at,
source.day_name
)