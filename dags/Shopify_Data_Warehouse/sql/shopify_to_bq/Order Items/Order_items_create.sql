
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items`
PARTITION BY DATE_TRUNC(Order_created_at,day)
CLUSTER BY Order_id
OPTIONS(
 description = "Order items table is partitioned on order created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
CAST(id AS STRING) as Order_id,
CAST(order_number AS STRING) as Order_number,
name as Order_name,
updated_at as Order_updated_at,
created_at as Order_created_at,
CAST(processed_at as TIMESTAMP) as Order_processed_at,
JSON_EXTRACT_SCALAR(customer, '$.id') AS customer_id,
CAST(JSON_EXTRACT_SCALAR(tax_lines, '$[0].price') AS FLOAT64) AS order_tax_price,
JSON_EXTRACT_SCALAR(item, '$.name') AS item_name,
  CAST(JSON_EXTRACT_SCALAR(item, '$.price') AS FLOAT64) AS item_price,
  CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS INT64) AS item_quantity,
  JSON_EXTRACT_SCALAR(item, '$.sku') AS item_sku_code,
  CAST(JSON_EXTRACT_SCALAR(item, '$.total_discount') AS FLOAT64) AS item_discount,
  CAST(JSON_EXTRACT_SCALAR(item, '$.variant_id') AS INT64) AS item_variant_id,
  JSON_EXTRACT_SCALAR(item, '$.fulfillment_status') AS item_fulfillment_status
FROM
 `shopify-pubsub-project.pilgrim_bi_airbyte.orders`,
  UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS item;
