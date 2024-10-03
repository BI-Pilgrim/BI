
MERGE INTO `shopify-pubsub-project.Shopify_staging.Refund_Order_items` AS target

USING (
  select 
  distinct
_airbyte_extracted_at,
Order_created_at,
Order_name,
customer_id,
Refund_created_at,
Refund_id,
Refund_note,
Refund_Order_id,
Refund_processed_at,
order_adjustments_amount,
order_adjustments_id,
order_adjustments_kind,
order_adjustments_reason,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.id') AS STRING) as item_refund_id,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.id') AS STRING) as order_item_id,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.name') as refund_item_name,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.price') as refund_item_price,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.product_id') as refund_product_id,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.sku') as refund_SKU_code,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.sku') as refund_item_quantity,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.tax_lines[0].price') AS FLOAT64) as item_tax_price1,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.tax_lines[0].rate') AS FLOAT64) as item_tax_rate1,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.tax_lines[0].title') as item_tax_type1,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.tax_lines[1].price') AS FLOAT64) as item_tax_price2,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.tax_lines[1].rate') AS FLOAT64) as item_tax_rate2,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.tax_lines[1].title') as item_tax_type2,
CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.total_discount') AS FLOAT64) as refund_item_discount_price,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.title') as refund_product_tile,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.variant_id') as refund_variant_id,
JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_item.variant_title') as refund_variant_title,

from (
select
_airbyte_extracted_at,
Order_created_at,
Order_name,
customer_id,
Refund_created_at,
Refund_id,
Refund_note,
Refund_Order_id,
Refund_processed_at,
order_adjustments_amount,
order_adjustments_id,
order_adjustments_kind,
order_adjustments_reason,
FULL_FLAT
FROM (
SELECT 
_airbyte_extracted_at,
created_at as Order_created_at,
name as Order_name,

CAST(JSON_EXTRACT_SCALAR(refunds, '$[0].created_at') AS TIMESTAMP) AS Refund_created_at,
JSON_EXTRACT_SCALAR(refunds, '$[0].id') AS Refund_id,
JSON_EXTRACT_SCALAR(refunds, '$[0].note') AS Refund_note,
JSON_EXTRACT_SCALAR(refunds, '$[0].order_id') AS Refund_Order_id,
CAST(JSON_EXTRACT_SCALAR(refunds, '$[0].processed_at') AS TIMESTAMP) AS Refund_processed_at,
CAST(JSON_EXTRACT_SCALAR(refunds, '$[0].order_adjustments.amount') AS FLOAT64) AS order_adjustments_amount,
JSON_EXTRACT_SCALAR(refunds, '$[0].order_adjustments.id') AS order_adjustments_id,
JSON_EXTRACT_SCALAR(refunds, '$[0].order_adjustments.kind4') AS order_adjustments_kind,
JSON_EXTRACT_SCALAR(refunds, '$[0].order_adjustments.reason') AS order_adjustments_reason,

CAST(JSON_EXTRACT_SCALAR(customer, '$.id') AS STRING) AS customer_id,
JSON_EXTRACT_ARRAY(JSON_EXTRACT(refunds,'$[0].refund_line_items')) as refund_item_flat,

FROM  `shopify-pubsub-project.airbyte711.orders`

), UNNEST (refund_item_flat) AS FULL_FLAT
)
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.item_refund_id = source.item_refund_id
and target.refund_variant_id = source.refund_variant_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Order_created_at = source.Order_created_at,
target.Order_name = source.Order_name,
target.customer_id = source.customer_id,
target.Refund_created_at = source.Refund_created_at,
target.Refund_id = source.Refund_id,
target.Refund_note = source.Refund_note,
target.Refund_Order_id = source.Refund_Order_id,
target.Refund_processed_at = source.Refund_processed_at,
target.order_adjustments_amount = source.order_adjustments_amount,
target.order_adjustments_id = source.order_adjustments_id,
target.order_adjustments_kind = source.order_adjustments_kind,
target.order_adjustments_reason = source.order_adjustments_reason,
target.item_refund_id = source.item_refund_id,
target.order_item_id = source.order_item_id,
target.refund_item_name = source.refund_item_name,
target.refund_item_price = source.refund_item_price,
target.refund_product_id = source.refund_product_id,
target.refund_SKU_code = source.refund_SKU_code,
target.refund_item_quantity = source.refund_item_quantity,
target.item_tax_price1 = source.item_tax_price1,
target.item_tax_rate1 = source.item_tax_rate1,
target.item_tax_type1 = source.item_tax_type1,
target.item_tax_price2 = source.item_tax_price2,
target.item_tax_rate2 = source.item_tax_rate2,
target.item_tax_type2 = source.item_tax_type2,
target.refund_item_discount_price = source.refund_item_discount_price,
target.refund_product_tile = source.refund_product_tile,
target.refund_variant_id = source.refund_variant_id,
target.refund_variant_title = source.refund_variant_title

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Order_created_at,
Order_name,
customer_id,
Refund_created_at,
Refund_id,
Refund_note,
Refund_Order_id,
Refund_processed_at,
order_adjustments_amount,
order_adjustments_id,
order_adjustments_kind,
order_adjustments_reason,
item_refund_id,
order_item_id,
refund_item_name,
refund_item_price,
refund_product_id,
refund_SKU_code,
refund_item_quantity,
item_tax_price1,
item_tax_rate1,
item_tax_type1,
item_tax_price2,
item_tax_rate2,
item_tax_type2,
refund_item_discount_price,
refund_product_tile,
refund_variant_id,
refund_variant_title
   )
  VALUES (
source._airbyte_extracted_at,
source.Order_created_at,
source.Order_name,
source.customer_id,
source.Refund_created_at,
source.Refund_id,
source.Refund_note,
source.Refund_Order_id,
source.Refund_processed_at,
source.order_adjustments_amount,
source.order_adjustments_id,
source.order_adjustments_kind,
source.order_adjustments_reason,
source.item_refund_id,
source.order_item_id,
source.refund_item_name,
source.refund_item_price,
source.refund_product_id,
source.refund_SKU_code,
source.refund_item_quantity,
source.item_tax_price1,
source.item_tax_rate1,
source.item_tax_type1,
source.item_tax_price2,
source.item_tax_rate2,
source.item_tax_type2,
source.refund_item_discount_price,
source.refund_product_tile,
source.refund_variant_id,
source.refund_variant_title

  )




