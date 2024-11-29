

CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Refund_Orders`
PARTITION BY DATE_TRUNC(Refund_created_at,day)
CLUSTER BY order_id, refund_status
OPTIONS(
 description = "Order refunds table is partitioned on  created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id as Refund_id,
note as Refund_note,
restock,
user_id,
order_id,
shop_url,
created_at as Refund_created_at,
processed_at as Refund_processed_at,
CAST(ROUND(CAST(JSON_EXTRACT_SCALAR(transactions[0], "$.amount") AS FLOAT64)) AS INT64) AS transaction_amount,
CAST(JSON_EXTRACT_SCALAR(transactions[0], "$.message") AS STRING) AS refund_message,
CAST(JSON_EXTRACT_SCALAR(transactions[0], "$.status") AS STRING) AS refund_status,
CAST(JSON_EXTRACT_SCALAR(transactions[0], "$.payment_id") AS STRING) AS payment_id,
CAST(JSON_EXTRACT_SCALAR(total_duties_set, '$.presentment_money.amount') AS FLOAT64) as presentment_amount,
CAST(JSON_EXTRACT_SCALAR(order_adjustments[0], "$.amount") AS FLOAT64) AS order_adjustment_amount,
CAST(JSON_EXTRACT_SCALAR(order_adjustments[0], "$.kind") AS STRING) AS order_adjustment_kind,
CAST(JSON_EXTRACT_SCALAR(order_adjustments[0], "$.tax_amount") AS FLOAT64) AS order_adjustment_tax,
CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.fulfillable_quantity") AS INT64) AS fulfillable_quantity,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.fulfillment_service") AS STRING) AS fulfillment_service,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.fulfillment_status") AS STRING) AS fulfillment_status,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.gift_card") AS BOOL) AS gift_card,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.name") AS STRING) AS refund_item_name,
  CAST(ROUND(CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.price") AS FLOAT64)) AS INT64) AS refund_item_price,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.product_id") AS INT64) AS refund_product_id,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.quantity") AS INT64) AS refund_item_quantity,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.sku") AS STRING) AS refund_sku_code,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.tax_lines[0].price") AS FLOAT64)AS tax_line_price,
  CAST(ROUND(CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.subtotal") AS FLOAT64)) AS INT64) AS refund_item_subtotal,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.total_tax") AS FLOAT64) AS total_tax,
  CAST(JSON_EXTRACT_SCALAR(refund_line_items[0], "$.line_item.vendor") AS STRING) AS order_refund_vendor,
admin_graphql_api_id
from `shopify-pubsub-project.pilgrim_bi_airbyte.order_refunds`
