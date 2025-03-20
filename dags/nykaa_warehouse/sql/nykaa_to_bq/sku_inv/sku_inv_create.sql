create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.sku_inv`
partition by mail_received_at
as


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
where rn = 1