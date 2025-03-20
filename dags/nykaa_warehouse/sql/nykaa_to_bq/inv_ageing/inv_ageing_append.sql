merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.inv_ageing` as target
using
(
select
* except(rn)
from
(
  select
  *,
  row_number() over(partition by ageing_bucket,reporting_week order by mail_received_at) as rn
  from
  (
    SELECT
    -- brand_name,

    CASE
      WHEN brand_name = 'Grand Total' AND ageing_bucket IS NULL THEN 'Grand Total'
      WHEN ageing_bucket IS NULL and brand_name = 'Pilgrim' THEN '0-3 m'
      -- WHN AGEING BUCKET IS NULL, THAT MEANS IT IS NEWLY INWARDED INVENTORY WHICH NEEDS TO BE MAPPED TO 0-3 m (as instructed by Zaid)
      ELSE ageing_bucket
    END AS ageing_bucket,

    sum(inventory_qty) as inventory_qty,
    sum(inventory_value) as inventory_value,
    -- pg_extracted_at,

    RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
    DATE(pg_mail_recieved_at) AS mail_received_at,
    FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS day_name

    FROM `shopify-pubsub-project.pilgrim_bi_nykaa.inv_ageing`
    group by all
  )
)
where rn = 1 and mail_received_at >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source

on target.ageing_bucket = source.ageing_bucket
and target.reporting_week = source.reporting_week

when matched and target.mail_received_at < source.mail_received_at
then update set

target.ageing_bucket = source.ageing_bucket,
target.inventory_qty = source.inventory_qty,
target.inventory_value = source.inventory_value,
target.reporting_week = source.reporting_week,
target.mail_received_at = source.mail_received_at,
target.day_name = source.day_name


when not matched
then insert
(
ageing_bucket,
inventory_qty,
inventory_value,
reporting_week,
mail_received_at,
day_name
)
values
(
source.ageing_bucket,
source.inventory_qty,
source.inventory_value,
source.reporting_week,
source.mail_received_at,
source.day_name

)