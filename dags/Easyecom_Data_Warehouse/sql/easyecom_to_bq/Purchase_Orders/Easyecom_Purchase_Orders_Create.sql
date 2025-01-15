CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Purchase_Orders`
PARTITION BY DATE_TRUNC(,day)
-- CLUSTER BY 
OPTIONS(
 description = "Purchase_Orders table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


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

  FROM `shopify-pubsub-project.easycom.purchase_orders`