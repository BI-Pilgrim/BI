
MERGE INTO `shopify-pubsub-project.Shopify_staging.Draft_Orders` AS target

USING (
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

  FROM `shopify-pubsub-project.airbyte711.draft_orders`
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.draft_order_id = source.draft_order_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.draft_order_status = source.draft_order_status,
target.draft_order_payment_terms = source.draft_order_payment_terms,
target.draft_order_total_tax = source.draft_order_total_tax,
target.draft_order_total_price = source.draft_order_total_price,
target.draft_order_tags = source.draft_order_tags,
target.draft_order_note = source.draft_order_note,
target.draft_order_email = source.draft_order_email,
target.draft_order_completed_at = source.draft_order_completed_at,
target.draft_order_id = source.draft_order_id,
target.draft_order_created_at = source.draft_order_created_at,
target.draft_order_updated_at = source.draft_order_updated_at,
target.draft_id = source.draft_id,
target.draft_order_name = source.draft_order_name,
target.draft_order_invoice_url = source.draft_order_invoice_url,
target.customer_id = source.customer_id,
target.discount_amount = source.discount_amount,
target.discount_title = source.discount_title,
target.discount_value = source.discount_value,
target.discount_value_type = source.discount_value_type

WHEN NOT MATCHED THEN INSERT (
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
discount_value_type
  )

  VALUES (
source._airbyte_extracted_at,
source.draft_order_status,
source.draft_order_payment_terms,
source.draft_order_total_tax,
source.draft_order_total_price,
source.draft_order_tags,
source.draft_order_note,
source.draft_order_email,
source.draft_order_completed_at,
source.draft_order_id,
source.draft_order_created_at,
source.draft_order_updated_at,
source.draft_id,
source.draft_order_name,
source.draft_order_invoice_url,
source.customer_id,
source.discount_amount,
source.discount_title,
source.discount_value,
source.discount_value_type
  )




