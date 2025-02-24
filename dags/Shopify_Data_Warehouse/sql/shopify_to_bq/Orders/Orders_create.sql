CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders`
PARTITION BY DATE_TRUNC(Order_created_at, day)
CLUSTER BY Order_fulfillment_status
OPTIONS(
  description = "Orders table is partitioned on Order date at day level",
  require_partition_filter = FALSE
)
AS 
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
    -- JSON_EXTRACT_SCALAR(discount_applications, '$[0].title') AS discount_application,
    ARRAY_TO_STRING(
    ARRAY(
      SELECT JSON_EXTRACT_SCALAR(element, '$.title') 
      FROM UNNEST(JSON_EXTRACT_ARRAY(discount_applications)) AS element
    ), ',') AS discount_application,
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
  o.admin_graphql_api_id, o.discount_application;
