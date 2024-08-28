CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Orders`
PARTITION BY DATE_TRUNC(Order_created_at,day)
CLUSTER BY Order_fulfillment_status
OPTIONS(
 description = "Orders table is partitioned on Order date at day level",
 require_partition_filter = FALSE
 )
 AS 
SELECT 
_airbyte_extracted_at,
note as Order_note,
cancel_reason as Order_cancelled_reason,
cancelled_at as Order_cancelled_at,
landing_site as landing_weburl,
referring_site as referring_weburl,
cart_token as Order_cart_token,
browser_ip as browser_ip_id,
checkout_id as Order_checkout_id,
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
app_id as Order_app_id,
total_line_items_price as total_line_items_price,
total_tax as total_tax,
total_price as Order_total_price,
total_discounts as Order_total_discounts,
tags as Order_tags,
updated_at as Order_updated_at,
created_at as Order_created_at,
processed_at as Order_processed_at,
name as Order_name,
id as Order_id,
token as Order_token,
order_number as Order_order_number,

JSON_EXTRACT(refunds, '$[0].created_at') AS Refund_created_at,
JSON_EXTRACT(refunds, '$[0].id') AS Refund_id,
JSON_EXTRACT(refunds, '$[0].note') AS Refund_note,
JSON_EXTRACT(refunds, '$[0].order_id') AS Refund_Order_id,
JSON_EXTRACT(refunds, '$[0].processed_at') AS Refund_processed_at,
JSON_EXTRACT(refunds, '$[0].order_adjustments.amount') AS order_adjustments_amount,
JSON_EXTRACT(refunds, '$[0].order_adjustments.id') AS order_adjustments_id,
JSON_EXTRACT(refunds, '$[0].order_adjustments.kind') AS order_adjustments_kind,
JSON_EXTRACT(refunds, '$[0].order_adjustments.reason') AS order_adjustments_reason,

JSON_EXTRACT(customer, '$.id') AS customer_id,

JSON_EXTRACT(tax_lines, '$[0].channel_liable') AS tax_channel_liable,
JSON_EXTRACT(tax_lines, '$[0].price') AS tax_price,
JSON_EXTRACT(tax_lines, '$[0].rate') AS tax_rate,
JSON_EXTRACT(tax_lines, '$[0].title') AS tax_title,

JSON_EXTRACT(client_details, '$.accept_language') AS browser_language,
JSON_EXTRACT(client_details, '$.browser_height') AS browser_height,
JSON_EXTRACT(client_details, '$.browser_ip') AS browser_ip,
JSON_EXTRACT(client_details, '$.browser_width') AS browser_width,
JSON_EXTRACT(client_details, '$.session_hash') AS session_hash,
JSON_EXTRACT(client_details, '$.user_agent') AS browser_agent,

JSON_EXTRACT(discount_codes, '$[0].amount') AS discount_amount,
JSON_EXTRACT(discount_codes, '$[0].code') AS discount_code,
JSON_EXTRACT(discount_codes, '$[0].type') AS discount_type,

JSON_EXTRACT(billing_address, '$.address1') AS billing_address,
JSON_EXTRACT(billing_address, '$.city') AS billing_city,
JSON_EXTRACT(billing_address, '$.country') AS billing_country,
JSON_EXTRACT(billing_address, '$.country_code') AS billing_country_code,
JSON_EXTRACT(billing_address, '$.province') AS billing_province,
JSON_EXTRACT(billing_address, '$.province_code') AS billing_province_code,
JSON_EXTRACT(billing_address, '$.zip') AS billing_zip,

JSON_EXTRACT(shipping_address, '$.address1') AS shipping_address,
JSON_EXTRACT(shipping_address, '$.city') AS shipping_city,
JSON_EXTRACT(shipping_address, '$.country') AS shipping_country,
JSON_EXTRACT(shipping_address, '$.country_code') AS shipping_country_code,
JSON_EXTRACT(shipping_address, '$.province') AS shipping_province,
JSON_EXTRACT(shipping_address, '$.province_code') AS shipping_province_code,
JSON_EXTRACT(shipping_address, '$.zip') AS shipping_zip,

JSON_EXTRACT(payment_gateway_names, '$') AS payment_gateway_names,

FROM  `shopify-pubsub-project.airbyte711.orders`

