

CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Draft_Orders`
PARTITION BY DATE_TRUNC(draft_order_created_at,day)
CLUSTER BY draft_order_status
OPTIONS(
 description = "Draft Order table is partitioned on draft order created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct 
_airbyte_extracted_at,
status as draft_order_status,
payment_terms as draft_order_payment_terms,
CAST(total_tax AS FLOAT64) as draft_order_total_tax,
CAST(total_price AS FLOAT64) as draft_order_total_price,
tags as draft_order_tags,
note as draft_order_note,
email as draft_order_email,
completed_at as draft_order_completed_at,
CAST(order_id AS STRING) as draft_order_id,
created_at as draft_order_created_at,
updated_at as draft_order_updated_at,
CAST(id AS STRING) as draft_id,
CAST(name AS STRING) as draft_order_name,
invoice_url as draft_order_invoice_url,

CAST(JSON_EXTRACT_SCALAR(customer, '$.id') AS STRING) AS customer_id,

CAST(JSON_EXTRACT_SCALAR(applied_discount, '$.amount') AS FLOAT64) AS discount_amount,
CAST(JSON_EXTRACT_SCALAR(applied_discount, '$.title') AS STRING) AS discount_title,
CAST(JSON_EXTRACT_SCALAR(applied_discount, '$.value') AS FLOAT64) AS discount_value,
CAST(JSON_EXTRACT_SCALAR(applied_discount, '$.value_type') AS STRING) AS discount_value_type,

FROM  `shopify-pubsub-project.airbyte711.draft_orders`

