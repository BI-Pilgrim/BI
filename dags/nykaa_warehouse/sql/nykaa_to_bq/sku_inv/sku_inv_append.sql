merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_inv` as target
using
(
select
* except(rn),
from
(
select
brand_name,
coalesce(`0_3_m`,0) as m0_m3,
coalesce(`3_6_m`,0) as m3_m6,
coalesce(`6_9_m`,0) as m6_m9,
coalesce(`9_m+`,0) as m9__,
grand_total,

RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
DATE(pg_mail_recieved_at) AS mail_received_at,
FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS report_day,
row_number() over(partition by brand_name,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn,
from `shopify-pubsub-project.pilgrim_bi_nykaa.sku_inv`
)
where rn = 1 and mail_received_at >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source

on target.brand_name = source.brand_name
and target.reporting_week = source.reporting_week

when matched and target.mail_received_at < source.mail_received_at
then update set

target.brand_name = source.brand_name,
target.m0_m3 = source.m0_m3,
target.m3_m6 = source.m3_m6,
target.m6_m9 = source.m6_m9,
target.m9__ = source.m9__,
target.grand_total = source.grand_total,
target.reporting_week = source.reporting_week,
target.mail_received_at = source.mail_received_at,
target.report_day = source.report_day

when not matched
then insert
(
  brand_name,
  m0_m3,
  m3_m6,
  m6_m9,
  m9__,
  grand_total,
  reporting_week,
  mail_received_at,
  report_day
)
values
(
  source.brand_name,
  source.m0_m3,
  source.m3_m6,
  source.m6_m9,
  source.m9__,
  source.grand_total,
  source.reporting_week,
  source.mail_received_at,
  source.report_day
)