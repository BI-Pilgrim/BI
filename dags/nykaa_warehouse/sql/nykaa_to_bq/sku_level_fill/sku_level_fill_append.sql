merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_level_fill` as target
using
(
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
where rn = 1 and mail_received_at >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source

on target.sku_code = source.sku_code
and target.pack_size = source.pack_size
and target.reporting_week = source.reporting_week

when matched and target.mail_received_at < source.mail_received_at
then update set

target.brand_name = source.brand_name,
target.sku_code = source.sku_code,
target.pack_size = source.pack_size,
target.dec = source.dec,
target.nov = source.nov,
target.oct = source.oct,
target.jan = source.jan,
target.grand_total = source.grand_total,
target.sku_desc = source.sku_desc,
target.reporting_week = source.reporting_week,
target.mail_received_at = source.mail_received_at,
target.report_day = source.report_day

when not matched
then insert
(
  brand_name,
  sku_code,
  pack_size,
  dec,
  nov,
  oct,
  jan,
  grand_total,
  sku_desc,
  reporting_week,
  mail_received_at,
  report_day
)
values
(
source.brand_name,
source.sku_code,
source.pack_size,
source.dec,
source.nov,
source.oct,
source.jan,
source.grand_total,
source.sku_desc,
source.reporting_week,
source.mail_received_at,
source.report_day
)