CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Orders`
PARTITION BY DATE_TRUNC(Order_created_at,day)
CLUSTER BY Order_fulfillment_status
OPTIONS(
 description = "Orders table is partitioned on Order date at day level",
 require_partition_filter = FALSE
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
note as Order_note,
cancel_reason as Order_cancelled_reason,
cancelled_at as Order_cancelled_at,
landing_site as landing_weburl,
referring_site as referring_weburl,
cart_token as Order_cart_token,
browser_ip as browser_ip_id,
CAST(checkout_id AS STRING) as Order_checkout_id,
checkout_token as Order_checkout_token,
closed_at as Order_closed_at,
fulfillment_status as Order_fulfillment_status,
currency as Order_currency,
shop_url as Order_shop_url,
confirmed as Order_confirmed,
estimated_taxes as Order_estimated_taxes,
test as isTestOrder,
taxes_included as Order_taxes_included,
financial_status as Order_financial_status,
source_name as Order_source_name,
CAST(app_id AS STRING) as Order_app_id,
CAST(total_line_items_price AS FLOAT64) as total_line_items_price,
CAST(total_tax AS FLOAT64) as total_tax,
CAST(total_price AS FLOAT64) as Order_total_price,
CAST(total_discounts AS FLOAT64) as Order_total_discounts,
tags as Order_tags,
updated_at as Order_updated_at,
created_at as Order_created_at,
CAST(processed_at as TIMESTAMP) as Order_processed_at,
name as Order_name,
CAST(id AS STRING) as Order_id,
token as Order_token,
CAST(order_number AS STRING) as Order_number,

JSON_EXTRACT_SCALAR(customer, '$.id') AS customer_id,

JSON_EXTRACT_SCALAR(tax_lines, '$[0].channel_liable') AS tax_channel_liable,
CAST(JSON_EXTRACT_SCALAR(tax_lines, '$[0].price') AS FLOAT64) AS tax_price,
CAST(JSON_EXTRACT_SCALAR(tax_lines, '$[0].rate') AS FLOAT64) AS tax_rate,
JSON_EXTRACT_SCALAR(tax_lines, '$[0].title') AS tax_title,

JSON_EXTRACT_SCALAR(client_details, '$.accept_language') AS browser_language,
JSON_EXTRACT_SCALAR(client_details, '$.browser_height') AS browser_height,
JSON_EXTRACT_SCALAR(client_details, '$.browser_ip') AS browser_ip,
JSON_EXTRACT_SCALAR(client_details, '$.browser_width') AS browser_width,
JSON_EXTRACT_SCALAR(client_details, '$.session_hash') AS session_hash,
JSON_EXTRACT_SCALAR(client_details, '$.user_agent') AS browser_agent,

CAST(JSON_EXTRACT_SCALAR(discount_codes, '$[0].amount') AS FLOAT64) AS discount_amount,
JSON_EXTRACT_SCALAR(discount_codes, '$[0].code') AS discount_code,
JSON_EXTRACT_SCALAR(discount_codes, '$[0].type') AS discount_type,

JSON_EXTRACT_SCALAR(billing_address, '$.address1') AS billing_address,
JSON_EXTRACT_SCALAR(billing_address, '$.city') AS billing_city,
JSON_EXTRACT_SCALAR(billing_address, '$.country') AS billing_country,
JSON_EXTRACT_SCALAR(billing_address, '$.country_code') AS billing_country_code,
JSON_EXTRACT_SCALAR(billing_address, '$.province') AS billing_province,
JSON_EXTRACT_SCALAR(billing_address, '$.province_code') AS billing_province_code,
JSON_EXTRACT_SCALAR(billing_address, '$.zip') AS billing_zip,

JSON_EXTRACT_SCALAR(shipping_address, '$.address1') AS shipping_address,
JSON_EXTRACT_SCALAR(shipping_address, '$.city') AS shipping_city,
JSON_EXTRACT_SCALAR(shipping_address, '$.country') AS shipping_country,
JSON_EXTRACT_SCALAR(shipping_address, '$.country_code') AS shipping_country_code,
JSON_EXTRACT_SCALAR(shipping_address, '$.province') AS shipping_province,
JSON_EXTRACT_SCALAR(shipping_address, '$.province_code') AS shipping_province_code,
JSON_EXTRACT_SCALAR(shipping_address, '$.zip') AS shipping_zip,

JSON_EXTRACT_SCALAR(payment_gateway_names, '$') AS payment_gateway_names,

FROM  `shopify-pubsub-project.airbyte711.orders`
