merge into `shopify-pubsub-project.Data_Warehouse_Nykaa_Staging.grn_details` as target
using
(
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
row_number() over(partition by sku_code,po_date order by pg_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_nykaa.grn_details
-- where sku_code = '8906120580052'
-- and po_date = '2024-11-18'
)
where rn = 1 and date(pg_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.sku_code = source.sku_code
and target.po_date = source.po_date
when matched and target.pg_extracted_at < source.pg_extracted_at
then update set
target.brand_name = source.brand_name,
target.location_name = source.location_name,
target.pocode = source.pocode,
target.sku_code = source.sku_code,
target.po_date = source.po_date,
target.grn_date_po_level = source.grn_date_po_level,
target.sku_velocity = source.sku_velocity,
target.grn_month_po_level = source.grn_month_po_level,
target.wh_location = source.wh_location,
target.po_qty = source.po_qty,
target.grn_qty = source.grn_qty,
target.pg_extracted_at = source.pg_extracted_at
when not matched
then insert
(
brand_name,
location_name,
pocode,
sku_code,
po_date,
grn_date_po_level,
sku_velocity,
grn_month_po_level,
wh_location,
po_qty,
grn_qty,
pg_extracted_at
)
values
(
source.brand_name,
source.location_name,
source.pocode,
source.sku_code,
source.po_date,
source.grn_date_po_level,
source.sku_velocity,
source.grn_month_po_level,
source.wh_location,
source.po_qty,
source.grn_qty,
source.pg_extracted_at
)