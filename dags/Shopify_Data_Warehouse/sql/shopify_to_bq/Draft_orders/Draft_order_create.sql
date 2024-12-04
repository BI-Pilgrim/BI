
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Draft_orders`
PARTITION BY DATE_TRUNC(Draft_order_created_at,day)
CLUSTER BY Draft_order_id
OPTIONS(
 description = "Draft Order table is partitioned on  Draft_order created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id,
name as Draft_order_name,
note as Draft_order_note,
tags as Draft_order_tag,
status as Draft_order_status,
currency,
CAST(JSON_EXTRACT_SCALAR(customer, "$.default_address.name") AS STRING) AS Customer_name,
CAST(JSON_EXTRACT_SCALAR(customer, "$.default_address.phone") AS STRING) AS Customer_phone,
CAST(JSON_EXTRACT_SCALAR(customer, "$.id") AS INT64) AS Customer_id,
CAST(JSON_EXTRACT_SCALAR(customer, "$.email") AS STRING) AS Customer_email,
order_id as Draft_order_id,
shop_url,
CAST(total_tax AS FLOAT64) AS total_tax,
created_at AS Draft_order_created_at,

CAST(JSON_EXTRACT_SCALAR(item, "$.name") AS STRING) AS Product_name,
CAST(JSON_EXTRACT_SCALAR(item, "$.sku") AS STRING) AS Product_SKU,
CAST(JSON_EXTRACT_SCALAR(item, "$.variant_id") AS INT64) AS Product_variant_id,
CAST(JSON_EXTRACT_SCALAR(item, "$.price") AS INT64) AS Product_price,
CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(item, "$.tax_lines[0]"), "$.price") AS FLOAT64) AS Product_tax_amount,
tax_exempt,
updated_at AS Draft_order_updated_at,
invoice_url,
CAST(total_price AS FLOAT64) AS Draft_order_amount,
completed_at AS Draft_order_completed_at,

CAST(REGEXP_EXTRACT(REPLACE(payment_terms, "'", '"'), r'"payment_terms_id":\s*(\d+)') AS INT64) AS Payment_terms_id,
CAST(REGEXP_EXTRACT(REPLACE(payment_terms, "'", '"'), r'"reference_id":\s*(\d+)') AS INT64) AS Payment_reference_id,
CAST(REGEXP_EXTRACT(REPLACE(payment_terms, "'", '"'), r'"total_balance":\s*"([\d\.]+)"') AS FLOAT64) AS Payment_balance_amount,


CAST(JSON_EXTRACT_SCALAR(shipping_line, "$.price") AS INT64) AS Shipping_price,
CAST(JSON_EXTRACT_SCALAR(shipping_line, "$.title") AS STRING) AS Shipping_title,

CAST(subtotal_price AS FLOAT64)	AS	Draft_order_subtotal,
taxes_included,
CONCAT(
  CAST(JSON_EXTRACT_SCALAR(billing_address, "$.address1") AS STRING), 
    ", ", 
  CAST(JSON_EXTRACT_SCALAR(billing_address, "$.address2") AS STRING)
  ) AS Billing_address,
invoice_sent_at,	
CAST(JSON_EXTRACT_SCALAR(applied_discount, "$.amount") AS STRING) AS	Discount_amount,
CONCAT(
  CAST(JSON_EXTRACT_SCALAR(shipping_address, "$.address1") AS STRING), 
    ", ", 
  CAST(JSON_EXTRACT_SCALAR(shipping_address, "$.address2") AS STRING)
  ) AS Shipping_address,

CAST(JSON_EXTRACT_SCALAR(shipping_address, "$.city") AS STRING) AS Shipping_city,
admin_graphql_api_id

from `shopify-pubsub-project.pilgrim_bi_airbyte.draft_orders`,
UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS item
