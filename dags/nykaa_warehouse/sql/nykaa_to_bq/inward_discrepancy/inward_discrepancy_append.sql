merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.inward_discrepancy` as target
using
(
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
)
where rn = 1 and mail_received_at >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source

on target.product_sku = source.product_sku
and target.reason = source.reason
and target.location_name = source.location_name
and target.reporting_week = source.reporting_week

when matched and target.mail_received_at < source.mail_received_at
then update set

target.product_sku = source.product_sku,
target.desctiption = source.desctiption,
target.brand_name = source.brand_name,
target.reason = source.reason,
target.location_name = source.location_name,
target.january = source.january,
target.pg_extracted_at = source.pg_extracted_at,
target.reporting_week = source.reporting_week,
target.mail_received_at = source.mail_received_at,
target.day_name = source.day_name


when not matched
then insert
(
product_sku,
desctiption,
brand_name,
reason,
location_name,
january,
pg_extracted_at,
reporting_week,
mail_received_at,
day_name
)
values
(
source.product_sku,
source.desctiption,
source.brand_name,
source.reason,
source.location_name,
source.january,
source.pg_extracted_at,
source.reporting_week,
source.mail_received_at,
source.day_name

)