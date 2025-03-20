create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_level_fill`
partition by mail_received_at
as

select
* except(rn),
from
(
  select
  brand_name,
  sku_code,
  pack_size,
  coalesce(dec,0) as dec,
  coalesce(nov,0) as nov,
  coalesce(oct,0) as oct,
  coalesce(jan,0) as jan,
  -- feb,
  -- mar,
  -- apr,
  -- may,
  -- jun,
  -- jul,
  -- aug,
  -- sep,
  coalesce(grand_total,0) as grand_total,
  coalesce(sku_desc,'-') as sku_desc,
  RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
  DATE(pg_mail_recieved_at) AS mail_received_at,
  FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS report_day,
  row_number() over(partition by sku_code,pack_size,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn,
  from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_level_fill`
)
where rn = 1