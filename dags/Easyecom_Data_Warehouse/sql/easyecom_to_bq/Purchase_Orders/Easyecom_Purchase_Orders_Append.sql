MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Purchase_Orders` AS TARGET
USING
(
select
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
  ee_extracted_at,
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY ee_extracted_at ORDER BY ee_extracted_at) AS row_num
FROM `shopify-pubsub-project.easycom.purchase_orders`
WHERE DATE(po_updated_date) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
) AS SOURCE
ON TARGET.po_id = SOURCE.po_id AND target.po_number = source.po_number
WHEN MATCHED AND TARGET.po_updated_date < SOURCE.po_updated_date
THEN UPDATE SET
TARGET.po_id = SOURCE.po_id,
TARGET.total_po_value = SOURCE.total_po_value,
TARGET.po_number = SOURCE.po_number,
TARGET.po_ref_num = SOURCE.po_ref_num,
TARGET.po_status_id = SOURCE.po_status_id,
TARGET.po_created_date = SOURCE.po_created_date,
TARGET.po_updated_date = SOURCE.po_updated_date,
TARGET.po_created_warehouse = SOURCE.po_created_warehouse,
TARGET.po_created_warehouse_c_id = SOURCE.po_created_warehouse_c_id,
TARGET.vendor_name = SOURCE.vendor_name,
TARGET.vendor_c_id = SOURCE.vendor_c_id,
TARGET.po_items = SOURCE.po_items,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
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
VALUES
(
SOURCE.po_id,
SOURCE.total_po_value,
SOURCE.po_number,
SOURCE.po_ref_num,
SOURCE.po_status_id,
SOURCE.po_created_date,
SOURCE.po_updated_date,
SOURCE.po_created_warehouse,
SOURCE.po_created_warehouse_c_id,
SOURCE.vendor_name,
SOURCE.vendor_c_id,
SOURCE.po_items,
SOURCE.ee_extracted_at
)