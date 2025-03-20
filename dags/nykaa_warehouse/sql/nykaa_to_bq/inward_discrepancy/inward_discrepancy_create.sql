create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.inward_discrepancy`
partition by mail_received_at
as

select
* except(rn)
from
(
  select
  product_sku,
  desctiption,
  brand_name,
  reason,
  location_name,
  january,
  pg_extracted_at,
  RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
  DATE(pg_mail_recieved_at) AS mail_received_at,
  FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS day_name,
  row_number() over(partition by product_sku,reason,location_name,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn
  from `shopify-pubsub-project.pilgrim_bi_nykaa.inward_discrepancy`
  -- where product_sku = 'PILGR00000215'
)
-- where reporting_week = 'Week-2'
where rn = 1
-- order by product_sku