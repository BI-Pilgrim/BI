
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` AS target
USING (
WITH source_orders AS (
  SELECT
    _airbyte_extracted_at,
    note AS Order_note,
    cancel_reason AS Order_cancelled_reason,
    cancelled_at AS Order_cancelled_at,
    landing_site AS landing_weburl,
    referring_site AS referring_weburl,
    cart_token AS Order_cart_token,
    browser_ip AS browser_ip_id,
    CAST(checkout_id AS STRING) AS Order_checkout_id,
    checkout_token AS Order_checkout_token,
    closed_at AS Order_closed_at,
    fulfillment_status AS Order_fulfillment_status,
    currency AS Order_currency,
    shop_url AS Order_shop_url,
    confirmed AS Order_confirmed,
    estimated_taxes AS Order_estimated_taxes,
    test AS isTestOrder,
    taxes_included AS Order_taxes_included,
    financial_status AS Order_financial_status,
    source_name AS Order_source_name,
    CAST(app_id AS STRING) AS Order_app_id,
    CAST(total_line_items_price AS FLOAT64) AS total_line_items_price,
    CAST(total_tax AS FLOAT64) AS total_tax,
    CAST(total_price AS FLOAT64) AS Order_total_price,
    CAST(total_discounts AS FLOAT64) AS Order_total_discounts,
    tags AS Order_tags,
    updated_at AS Order_updated_at,
    created_at AS Order_created_at,
    CAST(processed_at AS TIMESTAMP) AS Order_processed_at,
    name AS Order_name,
    CAST(id AS STRING) AS Order_id,
    token AS Order_token,
    CAST(order_number AS STRING) AS Order_number,

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

    admin_graphql_api_id,
    JSON_EXTRACT_SCALAR(discount_applications, '$[0].title') AS discount_application,
    -- Extract the payment_gateway_names JSON array as is
    payment_gateway_names

  FROM `shopify-pubsub-project.pilgrim_bi_airbyte.orders`
)

-- Aggregate multiple payment gateways into a single row per order
SELECT 
  o._airbyte_extracted_at, o.Order_note, o.Order_cancelled_reason, o.Order_cancelled_at, o.landing_weburl,
  o.referring_weburl, o.Order_cart_token, o.browser_ip_id, o.Order_checkout_id, o.Order_checkout_token, 
  o.Order_closed_at, o.Order_fulfillment_status, o.Order_currency, o.Order_shop_url, o.Order_confirmed, 
  o.Order_estimated_taxes, o.isTestOrder, o.Order_taxes_included, o.Order_financial_status, 
  o.Order_source_name, o.Order_app_id, o.total_line_items_price, o.total_tax, o.Order_total_price, 
  o.Order_total_discounts, o.Order_tags, o.Order_updated_at, o.Order_created_at, o.Order_processed_at, 
  o.Order_name, o.Order_id, o.Order_token, o.Order_number, o.customer_id, o.tax_channel_liable, 
  o.tax_price, o.tax_rate, o.tax_title, o.browser_language, o.browser_height, o.browser_ip, 
  o.browser_width, o.session_hash, o.browser_agent, o.discount_amount, o.discount_code, o.discount_type, 
  o.billing_address, o.billing_city, o.billing_country, o.billing_country_code, o.billing_province, 
  o.billing_province_code, o.billing_zip, o.shipping_address, o.shipping_city, o.shipping_country, 
  o.shipping_country_code, o.shipping_province, o.shipping_province_code, o.shipping_zip, 
  o.admin_graphql_api_id, o.discount_application,
  STRING_AGG(payment_gateway_name, ', ') AS payment_gateway_names
FROM source_orders AS o
LEFT JOIN UNNEST(JSON_VALUE_ARRAY(o.payment_gateway_names)) AS payment_gateway_name
GROUP BY 
  o._airbyte_extracted_at, o.Order_note, o.Order_cancelled_reason, o.Order_cancelled_at, o.landing_weburl,
  o.referring_weburl, o.Order_cart_token, o.browser_ip_id, o.Order_checkout_id, o.Order_checkout_token, 
  o.Order_closed_at, o.Order_fulfillment_status, o.Order_currency, o.Order_shop_url, o.Order_confirmed, 
  o.Order_estimated_taxes, o.isTestOrder, o.Order_taxes_included, o.Order_financial_status, 
  o.Order_source_name, o.Order_app_id, o.total_line_items_price, o.total_tax, o.Order_total_price, 
  o.Order_total_discounts, o.Order_tags, o.Order_updated_at, o.Order_created_at, o.Order_processed_at, 
  o.Order_name, o.Order_id, o.Order_token, o.Order_number, o.customer_id, o.tax_channel_liable, 
  o.tax_price, o.tax_rate, o.tax_title, o.browser_language, o.browser_height, o.browser_ip, 
  o.browser_width, o.session_hash, o.browser_agent, o.discount_amount, o.discount_code, o.discount_type, 
  o.billing_address, o.billing_city, o.billing_country, o.billing_country_code, o.billing_province, 
  o.billing_province_code, o.billing_zip, o.shipping_address, o.shipping_city, o.shipping_country, 
  o.shipping_country_code, o.shipping_province, o.shipping_province_code, o.shipping_zip, 
  o.admin_graphql_api_id, o.discount_application
 ) AS source
ON target.Order_id = source.Order_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Order_note = source.Order_note,
target.Order_cancelled_reason = source.Order_cancelled_reason,
target.Order_cancelled_at = source.Order_cancelled_at,
target.landing_weburl = source.landing_weburl,
target.referring_weburl = source.referring_weburl,
target.Order_cart_token = source.Order_cart_token,
target.browser_ip_id = source.browser_ip_id,
target.Order_checkout_id = source.Order_checkout_id,
target.Order_checkout_token = source.Order_checkout_token,
target.Order_closed_at = source.Order_closed_at,
target.Order_fulfillment_status = source.Order_fulfillment_status,
target.Order_currency = source.Order_currency,
target.Order_shop_url = source.Order_shop_url,
target.Order_confirmed = source.Order_confirmed,
target.Order_estimated_taxes = source.Order_estimated_taxes,
target.isTestOrder = source.isTestOrder,
target.Order_taxes_included = source.Order_taxes_included,
target.Order_financial_status = source.Order_financial_status,
target.Order_source_name = source.Order_source_name,
target.Order_app_id = source.Order_app_id,
target.total_line_items_price = source.total_line_items_price,
target.total_tax = source.total_tax,
target.Order_total_price = source.Order_total_price,
target.Order_total_discounts = source.Order_total_discounts,
target.Order_tags = source.Order_tags,
target.Order_updated_at = source.Order_updated_at,
target.Order_created_at = source.Order_created_at,
target.Order_processed_at = source.Order_processed_at,
target.Order_name = source.Order_name,
target.Order_id = source.Order_id,
target.Order_token = source.Order_token,
target.Order_number = source.Order_number,
target.customer_id = source.customer_id,
target.tax_channel_liable = source.tax_channel_liable,
target.tax_price = source.tax_price,
target.tax_rate = source.tax_rate,
target.tax_title = source.tax_title,
target.browser_language = source.browser_language,
target.browser_height = source.browser_height,
target.browser_ip = source.browser_ip,
target.browser_width = source.browser_width,
target.session_hash = source.session_hash,
target.browser_agent = source.browser_agent,
target.discount_amount = source.discount_amount,
target.discount_code = source.discount_code,
target.discount_type = source.discount_type,
target.billing_address = source.billing_address,
target.billing_city = source.billing_city,
target.billing_country = source.billing_country,
target.billing_country_code = source.billing_country_code,
target.billing_province = source.billing_province,
target.billing_province_code = source.billing_province_code,
target.billing_zip = source.billing_zip,
target.shipping_address = source.shipping_address,
target.shipping_city = source.shipping_city,
target.shipping_country = source.shipping_country,
target.shipping_country_code = source.shipping_country_code,
target.shipping_province = source.shipping_province,
target.shipping_province_code = source.shipping_province_code,
target.shipping_zip = source.shipping_zip,
target.payment_gateway_names = source.payment_gateway_names,
target.admin_graphql_api_id = source.admin_graphql_api_id,
target.discount_application = source.discount_application

WHEN NOT MATCHED THEN INSERT (
 _airbyte_extracted_at,
Order_note,
Order_cancelled_reason,
Order_cancelled_at,
landing_weburl,
referring_weburl,
Order_cart_token,
browser_ip_id,
Order_checkout_id,
Order_checkout_token,
Order_closed_at,
Order_fulfillment_status,
Order_currency,
Order_shop_url,
Order_confirmed,
Order_estimated_taxes,
isTestOrder,
Order_taxes_included,
Order_financial_status,
Order_source_name,
Order_app_id,
total_line_items_price,
total_tax,
Order_total_price,
Order_total_discounts,
Order_tags,
Order_updated_at,
Order_created_at,
Order_processed_at,
Order_name,
Order_id,
Order_token,
Order_number,
customer_id,
tax_channel_liable,
tax_price,
tax_rate,
tax_title,
browser_language,
browser_height,
browser_ip,
browser_width,
session_hash,
browser_agent,
discount_amount,
discount_code,
discount_type,
billing_address,
billing_city,
billing_country,
billing_country_code,
billing_province,
billing_province_code,
billing_zip,
shipping_address,
shipping_city,
shipping_country,
shipping_country_code,
shipping_province,
shipping_province_code,
shipping_zip,
payment_gateway_names,
admin_graphql_api_id,
discount_application
)
  VALUES (
source._airbyte_extracted_at,
source.Order_note,
source.Order_cancelled_reason,
source.Order_cancelled_at,
source.landing_weburl,
source.referring_weburl,
source.Order_cart_token,
source.browser_ip_id,
source.Order_checkout_id,
source.Order_checkout_token,
source.Order_closed_at,
source.Order_fulfillment_status,
source.Order_currency,
source.Order_shop_url,
source.Order_confirmed,
source.Order_estimated_taxes,
source.isTestOrder,
source.Order_taxes_included,
source.Order_financial_status,
source.Order_source_name,
source.Order_app_id,
source.total_line_items_price,
source.total_tax,
source.Order_total_price,
source.Order_total_discounts,
source.Order_tags,
source.Order_updated_at,
source.Order_created_at,
source.Order_processed_at,
source.Order_name,
source.Order_id,
source.Order_token,
source.Order_number,
source.customer_id,
source.tax_channel_liable,
source.tax_price,
source.tax_rate,
source.tax_title,
source.browser_language,
source.browser_height,
source.browser_ip,
source.browser_width,
source.session_hash,
source.browser_agent,
source.discount_amount,
source.discount_code,
source.discount_type,
source.billing_address,
source.billing_city,
source.billing_country,
source.billing_country_code,
source.billing_province,
source.billing_province_code,
source.billing_zip,
source.shipping_address,
source.shipping_city,
source.shipping_country,
source.shipping_country_code,
source.shipping_province,
source.shipping_province_code,
source.shipping_zip,
source.admin_graphql_api_id,
source.payment_gateway_names,
source.discount_application
  )
