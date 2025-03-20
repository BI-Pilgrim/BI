create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.open_rtv`
partition by mail_received_at
as


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
where rn = 1