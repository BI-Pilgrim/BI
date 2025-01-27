merge into`shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.grn_details_report` as target
using
(
select *
from
(
select distinct
*,
row_number() over(partition by poNo, companyProductId, receivedQty order by created_on desc) as rn
from shopify-pubsub-project.easycom.grn_details_report
)
where rn = 1 and date(created_on) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on  target.poNo = source.poNo
and target.companyProductId = source.companyProductId
and target.receivedQty = source.receivedQty
when matched and target.created_on < source.created_on
then update set
target.warehouseLocation = source.warehouseLocation,
target.poNo = source.poNo,
target.poDate = source.poDate,
target.Mrp = source.Mrp,
target.poRef = source.poRef,
target.grnNo = source.grnNo,
target.grnDetailsDate = source.grnDetailsDate,
target.vendorName = source.vendorName,
target.vendorInvoiceNo = source.vendorInvoiceNo,
target.vendorInvoiceDate = source.vendorInvoiceDate,
target.productName = source.productName,
target.companyProductId = source.companyProductId,
target.sku = source.sku,
target.ean = source.ean,
target.unitPrice = source.unitPrice,
target.taxableAmount = source.taxableAmount,
target.poStatus = source.poStatus,
target.poQty = source.poQty,
target.receivedQty = source.receivedQty,
target.receivedValue = source.receivedValue,
target.userName = source.userName,
target.batchCode = source.batchCode,
target.exipryDate = source.exipryDate,
target.report_id = source.report_id,
target.report_type = source.report_type,
target.start_date = source.start_date,
target.end_date = source.end_date,
target.created_on = source.created_on,
target.inventory_type = source.inventory_type,
target.ee_extracted_at = source.ee_extracted_at
when not matched
then insert
(
warehouseLocation,
poNo,
poDate,
Mrp,
poRef,
grnNo,
grnDetailsDate,
vendorName,
vendorInvoiceNo,
vendorInvoiceDate,
productName,
companyProductId,
sku,
ean,
unitPrice,
taxableAmount,
poStatus,
poQty,
receivedQty,
receivedValue,
userName,
batchCode,
exipryDate,
report_id,
report_type,
start_date,
end_date,
created_on,
inventory_type,
ee_extracted_at
)
values
(
source.warehouseLocation,
source.poNo,
source.poDate,
source.Mrp,
source.poRef,
source.grnNo,
source.grnDetailsDate,
source.vendorName,
source.vendorInvoiceNo,
source.vendorInvoiceDate,
source.productName,
source.companyProductId,
source.sku,
source.ean,
source.unitPrice,
source.taxableAmount,
source.poStatus,
source.poQty,
source.receivedQty,
source.receivedValue,
source.userName,
source.batchCode,
source.exipryDate,
source.report_id,
source.report_type,
source.start_date,
source.end_date,
source.created_on,
source.inventory_type,
source.ee_extracted_at
)