CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Order_items`
PARTITION BY DATE_TRUNC(order_date,day)
-- CLUSTER BY 
OPTIONS(
 description = "All Return Order item table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select
  CAST(invoice_id AS STRING) AS invoice_id,
  CAST(order_id AS STRING) AS order_id,
  reference_code,
  order_date,

  JSON_EXTRACT_SCALAR(item_flat,'$[company_product_id]') AS company_product_id,
  JSON_EXTRACT_SCALAR(item_flat,'$[product_id]') AS product_id,
  JSON_EXTRACT_SCALAR(item_flat,'$[suborder_id]') AS suborder_id,
  JSON_EXTRACT_SCALAR(item_flat,'$[suborder_num]') AS suborder_num,
  JSON_EXTRACT_SCALAR(item_flat,'$[return_reason]') AS return_reason,
  JSON_EXTRACT_SCALAR(item_flat,'$[inventory_status]') AS inventory_status,
  JSON_EXTRACT_SCALAR(item_flat,'$[shipment_type]') AS shipment_type,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[suborder_quantity]') AS FLOAT64) AS suborder_quantity,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[returned_item_quantity]') AS FLOAT64) AS returned_item_quantity,
  JSON_EXTRACT_SCALAR(item_flat,'$[tax_type]') AS tax_type,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[total_item_selling_price]') AS FLOAT64) AS total_item_selling_price,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[credit_note_total_item_shipping_charge]') AS FLOAT64) AS credit_note_total_item_shipping_charge,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[credit_note_total_item_miscellaneous]') AS FLOAT64) AS credit_note_total_item_miscellaneous,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[item_tax_rate]') AS FLOAT64) AS item_tax_rate,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[credit_note_total_item_tax]') AS FLOAT64) AS credit_note_total_item_tax,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[credit_note_total_item_excluding_tax]') AS FLOAT64) AS credit_note_total_item_excluding_tax,
  JSON_EXTRACT_SCALAR(item_flat,'$[sku]') AS sku,
  JSON_EXTRACT_SCALAR(item_flat,'$[productName]') AS productName,
  JSON_EXTRACT_SCALAR(item_flat,'$[description]') AS description,
  JSON_EXTRACT_SCALAR(item_flat,'$[category]') AS category,
  JSON_EXTRACT_SCALAR(item_flat,'$[brand]') AS brand,
  JSON_EXTRACT_SCALAR(item_flat,'$[model_no]') AS model_no,
  JSON_EXTRACT_SCALAR(item_flat,'$[product_tax_code]') AS product_tax_code,
  JSON_EXTRACT_SCALAR(item_flat,'$[AccountingSku]') AS AccountingSku,
  JSON_EXTRACT_SCALAR(item_flat,'$[accounting_unit]') AS accounting_unit,
  JSON_EXTRACT_SCALAR(item_flat,'$[ean]') AS ean,
  JSON_EXTRACT_SCALAR(item_flat,'$[size]') AS size,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[cost]') AS FLOAT64) AS cost,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[mrp]') AS FLOAT64) AS mrp,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[weight]') AS FLOAT64) AS weight,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[length]') AS FLOAT64) AS length,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[width]') AS FLOAT64) AS width,
  CAST(JSON_EXTRACT_SCALAR(item_flat,'$[height]') AS FLOAT64) AS height,
  JSON_EXTRACT_SCALAR(item_flat,'$[item_type]') AS item_type,
  JSON_EXTRACT_SCALAR(item_flat,'$[parent_sku]') AS parent_sku,
  JSON_EXTRACT_SCALAR(item_flat,'$[gatepass_number]') AS gatepass_number,
  JSON_EXTRACT_SCALAR(item_flat,'$[meta]') AS meta,
  ee_extracted_at
  
from (
SELECT
  distinct
    invoice_id,
    order_id,
    reference_code,
    order_date,
    ee_extracted_at,
    JSON_QUERY_ARRAY(items,'$') AS items_array
  FROM `shopify-pubsub-project.easycom.all_return_orders`),UNNEST(items_array) AS item_flat