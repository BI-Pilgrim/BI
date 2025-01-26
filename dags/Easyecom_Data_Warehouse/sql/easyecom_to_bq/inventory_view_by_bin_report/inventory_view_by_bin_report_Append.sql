merge into `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.inventory_view_by_bin_report` as target
using
(
select
*
from
(
select
*,
row_number() over(partition by SKU order by ee_extracted_at desc) as rn
from shopify-pubsub-project.easycom.inventory_view_by_bin_report
)
where rn =1 and date(created_on) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) as source
on target.SKU = source.SKU
when matched and target.created_on < source.created_on
then update set
target.Report_Generated_Date = source.Report_Generated_Date,
target.SKU = source.SKU,
target.marketplaceSku = source.marketplaceSku,
target.EAN = source.EAN,
target.Product_Title = source.Product_Title,
target.Category = source.Category,
target.Brand = source.Brand,
target.MRP = source.MRP,
target.Bin = source.Bin,
target.Batch_Code = source.Batch_Code,
target.Expiry_Date = source.Expiry_Date,
target.Repair_Quantity = source.Repair_Quantity,
target.Color = source.Color,
target.Size = source.Size,
target.Manufacturing_Date = source.Manufacturing_Date,
target.Zone = source.Zone,
target.Shelf_Status = source.Shelf_Status,
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
Report_Generated_Date,
SKU,
marketplaceSku,
EAN,
Product_Title,
Category,
Brand,
MRP,
Bin,
Batch_Code,
Expiry_Date,
Repair_Quantity,
Color,
Size,
Manufacturing_Date,
Zone,
Shelf_Status,
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
source.Report_Generated_Date,
source.SKU,
source.marketplaceSku,
source.EAN,
source.Product_Title,
source.Category,
source.Brand,
source.MRP,
source.Bin,
source.Batch_Code,
source.Expiry_Date,
source.Repair_Quantity,
source.Color,
source.Size,
source.Manufacturing_Date,
source.Zone,
source.Shelf_Status,
source.report_id,
source.report_type,
source.start_date,
source.end_date,
source.created_on,
source.inventory_type,
source.ee_extracted_at
)