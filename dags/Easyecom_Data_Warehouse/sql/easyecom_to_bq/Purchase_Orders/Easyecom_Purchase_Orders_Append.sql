merge into `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Purchase_Orders` as target
using
(
select *
from
(
select distinct
*,
row_number() over(partition by po_id order by ee_extracted_at desc) as rn
from shopify-pubsub-project.easycom.purchase_orders
)
where rn =1 and date(po_created_date) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) as source
on target.po_id = source.po_id
when matched and target.po_created_date < source.po_created_date
then update set
target.po_id = source.po_id,
target.total_po_value = source.total_po_value,
target.po_number = source.po_number,
target.po_ref_num = source.po_ref_num,
target.po_status_id = source.po_status_id,
target.po_created_date = source.po_created_date,
target.po_updated_date = source.po_updated_date,
target.po_created_warehouse = source.po_created_warehouse,
target.po_created_warehouse_c_id = source.po_created_warehouse_c_id,
target.vendor_name = source.vendor_name,
target.vendor_c_id = source.vendor_c_id,
target.po_items = source.po_items,
target.ee_extracted_at = source.ee_extracted_at
when not matched
then insert
(
po_id,
total_po_value,
po_number,
po_ref_num,
po_status_id,
po_created_date,
po_updated_date,
po_created_warehouse,
po_created_warehouse_c_id,
vendor_name,
vendor_c_id,
po_items,
ee_extracted_at
)
values
(
source.po_id,
source.total_po_value,
source.po_number,
source.po_ref_num,
source.po_status_id,
source.po_created_date,
source.po_updated_date,
source.po_created_warehouse,
source.po_created_warehouse_c_id,
source.vendor_name,
source.vendor_c_id,
source.po_items,
source.ee_extracted_at
);