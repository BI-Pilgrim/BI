
MERGE INTO `shopify-pubsub-project.Shopify_staging.Order_items` AS target

USING (
  select 
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
 
 ) AS source
ON target.Order_name = source.Order_name
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
   
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Order_created_at = source.Order_created_at,
target.Order_name = source.Order_name,
target.customer_id = source.customer_id,
target.item_discount_amount = source.item_discount_amount,
target.item_fulfillable_quantity = source.item_fulfillable_quantity,
target.fulfillment_status = source.fulfillment_status,
target.order_item_id = source.order_item_id,
target.order_item_name = source.order_item_name,
target.product_id = source.product_id,
target.order_item_SKU_code = source.order_item_SKU_code,
target.order_item_type = source.order_item_type,
target.product_tile = source.product_tile,
target.variant_id = source.variant_id,
target.variant_title = source.variant_title,
target.order_item_price = source.order_item_price,
target.order_item_quantity = source.order_item_quantity,
target.order_item_tax_price = source.order_item_tax_price,
target.order_item_tax_rate = source.order_item_tax_rate,
target.item_total_discount = source.item_total_discount

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Order_created_at,
Order_name,
customer_id,
item_discount_amount,
item_fulfillable_quantity,
fulfillment_status,
order_item_id,
order_item_name,
product_id,
order_item_SKU_code,
order_item_type,
product_tile,
variant_id,
variant_title,
order_item_price,
order_item_quantity,
order_item_tax_price,
order_item_tax_rate,
item_total_discount
   )
   
  VALUES (
source._airbyte_extracted_at,
source.Order_created_at,
source.Order_name,
source.customer_id,
source.item_discount_amount,
source.item_fulfillable_quantity,
source.fulfillment_status,
source.order_item_id,
source.order_item_name,
source.product_id,
source.order_item_SKU_code,
source.order_item_type,
source.product_tile,
source.variant_id,
source.variant_title,
source.order_item_price,
source.order_item_quantity,
source.order_item_tax_price,
source.order_item_tax_rate,
source.item_total_discount

  )




