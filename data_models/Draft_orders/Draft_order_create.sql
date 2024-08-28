

CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Draft_Orders`
PARTITION BY DATE_TRUNC(draft_order_created_at,day)
CLUSTER BY draft_order_status
OPTIONS(
 description = "Draft Order table is partitioned on draft order created at",
 require_partition_filter = False
 )
 AS 
SELECT 
_airbyte_extracted_at,
status as draft_order_status,
payment_terms as draft_order_payment_terms,
total_tax as draft_order_total_tax,
total_price as draft_order_total_price,
tags as draft_order_tags,
note as draft_order_note,
email as draft_order_email,
completed_at as draft_order_completed_at,
order_id as draft_order_order_id,
created_at as draft_order_created_at,
updated_at as draft_order_updated_at,
id as draft_order_id,
name as draft_order_name,
invoice_url as draft_order_invoice_url,

JSON_EXTRACT(customer, '$.id') AS customer_id,

JSON_EXTRACT(applied_discount, '$.amount') AS discount_amount,
JSON_EXTRACT(applied_discount, '$.title') AS discount_title,
JSON_EXTRACT(applied_discount, '$.value') AS discount_value,
JSON_EXTRACT(applied_discount, '$.value_type') AS discount_value_type,

FROM  `shopify-pubsub-project.airbyte711.draft_orders`

