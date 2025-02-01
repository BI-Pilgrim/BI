merge into `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details` as target
using
(
select
*
from
(
select
*,
row_number() over(partition by grn_id order by ee_extracted_at DESC) as rn
from shopify-pubsub-project.easycom.grn_details
)
where rn = 1
) as source
on target.grn_id = source.grn_id
when matched and target.ee_extracted_at < source.ee_extracted_at
then update set
target.grn_id = source.grn_id,
target.grn_invoice_number = source.grn_invoice_number,
target.total_grn_value = source.total_grn_value,
target.grn_status_id = source.grn_status_id,
target.grn_status = source.grn_status,
target.grn_created_at = source.grn_created_at,
target.grn_invoice_date = source.grn_invoice_date,
target.po_id = source.po_id,
target.po_number = source.po_number,
target.po_ref_num = source.po_ref_num,
target.po_status_id = source.po_status_id,
target.po_created_date = source.po_created_date,
target.po_updated_date = source.po_updated_date,
target.inwarded_warehouse = source.inwarded_warehouse,
target.inwarded_warehouse_c_id = source.inwarded_warehouse_c_id,
target.vendor_name = source.vendor_name,
target.vendor_c_id = source.vendor_c_id,
target.grn_items = source.grn_items,
target.ee_extracted_at = source.ee_extracted_at
when not matched
then insert
(
grn_id,
grn_invoice_number,
total_grn_value,
grn_status_id,
grn_status,
grn_created_at,
grn_invoice_date,
po_id,
po_number,
po_ref_num,
po_status_id,
po_created_date,
po_updated_date,
inwarded_warehouse,
inwarded_warehouse_c_id,
vendor_name,
vendor_c_id,
grn_items,
ee_extracted_at
)
values
(
source.grn_id,
source.grn_invoice_number,
source.total_grn_value,
source.grn_status_id,
source.grn_status,
source.grn_created_at,
source.grn_invoice_date,
source.po_id,
source.po_number,
source.po_ref_num,
source.po_status_id,
source.po_created_date,
source.po_updated_date,
source.inwarded_warehouse,
source.inwarded_warehouse_c_id,
source.vendor_name,
source.vendor_c_id,
source.grn_items,
source.ee_extracted_at
);