
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Draft_orders` AS target  
USING (
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

CAST(subtotal_price AS FLOAT64) AS  Draft_order_subtotal,
taxes_included,
CONCAT(
  CAST(JSON_EXTRACT_SCALAR(billing_address, "$.address1") AS STRING), 
    ", ", 
  CAST(JSON_EXTRACT_SCALAR(billing_address, "$.address2") AS STRING)
  ) AS Billing_address,
invoice_sent_at,  
CAST(JSON_EXTRACT_SCALAR(applied_discount, "$.amount") AS STRING) AS  Discount_amount,
CONCAT(
  CAST(JSON_EXTRACT_SCALAR(shipping_address, "$.address1") AS STRING), 
    ", ", 
  CAST(JSON_EXTRACT_SCALAR(shipping_address, "$.address2") AS STRING)
  ) AS Shipping_address,

CAST(JSON_EXTRACT_SCALAR(shipping_address, "$.city") AS STRING) AS Shipping_city,
admin_graphql_api_id

from `shopify-pubsub-project.pilgrim_bi_airbyte.draft_orders`,
UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS item

WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.Draft_order_id = source.Draft_order_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.id = source.id,
target.Draft_order_name = source.Draft_order_name,
target.Draft_order_note = source.Draft_order_note,
target.Draft_order_tag = source.Draft_order_tag,
target.Draft_order_status = source.Draft_order_status,
target.currency = source.currency,
target.Customer_name = source.Customer_name,
target.Customer_phone = source.Customer_phone,
target.Customer_id = source.Customer_id,
target.Customer_email = source.Customer_email,
target.Draft_order_id = source.Draft_order_id,
target.shop_url = source.shop_url,
target.total_tax = source.total_tax,
target.Draft_order_created_at = source.Draft_order_created_at,
target.Product_name = source.Product_name,
target.Product_SKU = source.Product_SKU,
target.Product_variant_id = source.Product_variant_id,
target.Product_price = source.Product_price,
target.Product_tax_amount = source.Product_tax_amount,
target.tax_exempt = source.tax_exempt,
target.Draft_order_updated_at = source.Draft_order_updated_at,
target.invoice_url = source.invoice_url,
target.Draft_order_amount = source.Draft_order_amount,
target.Draft_order_completed_at = source.Draft_order_completed_at,
target.Payment_terms_id = source.Payment_terms_id,
target.Payment_reference_id = source.Payment_reference_id,
target.Payment_balance_amount = source.Payment_balance_amount,
target.Shipping_price = source.Shipping_price,
target.Shipping_title = source.Shipping_title,
target.Draft_order_subtotal = source.Draft_order_subtotal,
target.taxes_included = source.taxes_included,
target.Billing_address = source.Billing_address,
target.invoice_sent_at = source.invoice_sent_at,
target.Discount_amount = source.Discount_amount,
target.Shipping_address = source.Shipping_address,
target.Shipping_city = source.Shipping_city,
target.admin_graphql_api_id = source.admin_graphql_api_id


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
id,
Draft_order_name,
Draft_order_note,
Draft_order_tag,
Draft_order_status,
currency,
Customer_name,
Customer_phone,
Customer_id,
Customer_email,
Draft_order_id,
shop_url,
total_tax,
Draft_order_created_at,
Product_name,
Product_SKU,
Product_variant_id,
Product_price,
Product_tax_amount,
tax_exempt,
Draft_order_updated_at,
invoice_url,
Draft_order_amount,
Draft_order_completed_at,
Payment_terms_id,
Payment_reference_id,
Payment_balance_amount,
Shipping_price,
Shipping_title,
Draft_order_subtotal,
taxes_included,
Billing_address,
invoice_sent_at,
Discount_amount,
Shipping_address,
Shipping_city,
admin_graphql_api_id
    
  )
  VALUES (
source._airbyte_extracted_at,
source.id,
source.Draft_order_name,
source.Draft_order_note,
source.Draft_order_tag,
source.Draft_order_status,
source.currency,
source.Customer_name,
source.Customer_phone,
source.Customer_id,
source.Customer_email,
source.Draft_order_id,
source.shop_url,
source.total_tax,
source.Draft_order_created_at,
source.Product_name,
source.Product_SKU,
source.Product_variant_id,
source.Product_price,
source.Product_tax_amount,
source.tax_exempt,
source.Draft_order_updated_at,
source.invoice_url,
source.Draft_order_amount,
source.Draft_order_completed_at,
source.Payment_terms_id,
source.Payment_reference_id,
source.Payment_balance_amount,
source.Shipping_price,
source.Shipping_title,
source.Draft_order_subtotal,
source.taxes_included,
source.Billing_address,
source.invoice_sent_at,
source.Discount_amount,
source.Shipping_address,
source.Shipping_city,
source.admin_graphql_api_id

  );
