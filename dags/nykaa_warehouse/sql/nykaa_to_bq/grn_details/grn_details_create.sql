create or replace table `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.grn_details`
partition by po_date
as

select * except(rn)
from
(
select distinct
brand_name,
location_name,
pocode,
sku_code,
cast(po_date as date) as po_date,
cast(grn_date___po_level as date) as grn_date_po_level,
sku_velocity,
grn_month_po_level,
wh_location,
cast(po_qty as int64) as po_qty,
cast(grn_qty as int64) as grn_qty,
pg_extracted_at,
RIGHT(TRIM(pg_mail_subject), 6) AS reporting_week,
DATE(pg_mail_recieved_at) AS mail_received_at,
FORMAT_TIMESTAMP('%A', pg_mail_recieved_at) AS day_name,
row_number() over(partition by pocode,sku_code,po_date,RIGHT(TRIM(pg_mail_subject), 6) order by DATE(pg_mail_recieved_at)) as rn
from shopify-pubsub-project.pilgrim_bi_nykaa.grn_details
-- where sku_code = '8906120580052'
-- and po_date = '2024-11-18'
)
where rn = 1
-- order by sku_code,po_date


