CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Order_items`
PARTITION BY DATE_TRUNC(Order_created_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Orders item table is partitioned on Order date at day level",
 require_partition_filter = FALSE
 )
 AS 
select 
distinct
_airbyte_extracted_at,
Order_created_at,
Order_name,
customer_id,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.discount_allocations[0].amount') AS FLOAT64) as item_discount_amount,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.fulfillable_quantity') AS FLOAT64) as item_fulfillable_quantity,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.fulfillment_status') as fulfillment_status,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.id') as order_item_id,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.name') as order_item_name,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.product_id') as product_id,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.sku') as order_item_SKU_code,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].title') as order_item_type,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.title') as product_tile,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.variant_id') as variant_id,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.variant_title') as variant_title,

CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.price') AS FLOAT64) as order_item_price,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.quantity') AS FLOAT64) as order_item_quantity,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].price') AS FLOAT64) as order_item_tax_price,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].rate') AS FLOAT64) as order_item_tax_rate,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.total_discount') AS FLOAT64) as item_total_discount,

from (
select
_airbyte_extracted_at,
Order_created_at,
Order_name,
customer_id,
FULL_FLAT
FROM (
SELECT 
_airbyte_extracted_at,
created_at as Order_created_at,
name as Order_name,
CAST(JSON_EXTRACT_SCALAR(customer, '$.id') AS STRING) AS customer_id,
JSON_EXTRACT_ARRAY(line_items) as line_item_flat,

FROM  `shopify-pubsub-project.airbyte711.orders`

), UNNEST (line_item_flat) AS FULL_FLAT
)
