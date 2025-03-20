create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.inv_ageing`
partition by mail_received_at
as

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
where rn = 1
-- order by ageing_bucket,reporting_week