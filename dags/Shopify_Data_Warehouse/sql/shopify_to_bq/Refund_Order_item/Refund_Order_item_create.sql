
CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Refund_Order_items`

PARTITION BY DATE_TRUNC(Order_created_at,day)
-- CLUSTER BY 

OPTIONS(
 description = "Refund Orders item table is partitioned on Order date at day level",
 require_partition_filter = FALSE
 )
 AS 
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
