

CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Draft_Order_items`
PARTITION BY DATE_TRUNC(draft_order_created_at,day)
CLUSTER BY draft_order_status
OPTIONS(
 description = "Draft Order table is partitioned on draft order created at",
 require_partition_filter = False
 )
 AS 
select 

    _airbyte_extracted_at,
    draft_order_status,
    draft_order_payment_terms,
    draft_order_total_tax,
    draft_order_total_price,
    draft_order_tags,
    draft_order_note,
    draft_order_email,
    draft_order_completed_at,
    draft_order_id,
    draft_order_created_at,
    draft_order_updated_at,
    draft_id,
    draft_order_name,
    draft_order_invoice_url,
    customer_id,
    discount_amount,
    discount_title,
    discount_value,
    discount_value_type,

    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].amount') AS FLOAT64) as item_discount_amount,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].application_type') AS STRING) as item_application_type,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].created_at') AS TIMESTAMP) as item_discount_created_at,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].description') AS STRING) as item_discount_description,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].discount_class') AS STRING) as item_discount_class,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.applied_discounts[0].id') AS STRING) as item_discount_id,
   
    

    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.compare_at_price') AS FLOAT64) as item_compare_at_price,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.line_price') AS FLOAT64) as item_line_price,
    JSON_EXTRACT_SCALAR(Full_FLAT,'$.presentment_title') as item_presentment_title,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.price') AS FLOAT64) as item_price,
    JSON_EXTRACT_SCALAR(Full_FLAT,'$.product_id') as product_id,
    JSON_EXTRACT_SCALAR(Full_FLAT,'$.sku') as item_SKU_code,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.quantity') AS FLOAT64) as item_quantity,

    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].price') AS FLOAT64) as tax_line_price,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].rate') AS FLOAT64 )as tax_line_rate,
    JSON_EXTRACT_SCALAR(Full_FLAT,'$.tax_lines[0].title') as tax_line_title,

    JSON_EXTRACT_SCALAR(Full_FLAT,'$.title') as product_title,

    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.variant_id') AS STRING) as item_variant_id,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.variant_price') AS FLOAT64) as item_variant_price,
    CAST(JSON_EXTRACT_SCALAR(Full_FLAT,'$.variant_title') AS STRING) as item_variant_title,

from (
select
    _airbyte_extracted_at,
    draft_order_status,
    draft_order_payment_terms,
    draft_order_total_tax,
    draft_order_total_price,
    draft_order_tags,
    draft_order_note,
    draft_order_email,
    draft_order_completed_at,
    draft_order_id,
    draft_order_created_at,
    draft_order_updated_at,
    draft_id,
    draft_order_name,
    draft_order_invoice_url,
    customer_id,
    discount_amount,
    discount_title,
    discount_value,
    discount_value_type,
    FULL_FLAT,
FROM (
SELECT 
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
JSON_EXTRACT_ARRAY(line_items) as line_item_flat,

FROM  `shopify-pubsub-project.airbyte711.draft_orders`

), UNNEST (line_item_flat) AS FULL_FLAT
)
