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
    REGEXP_EXTRACT(landing_site, r"utm_source=([^&]+)") AS utm_source,
    REGEXP_EXTRACT(landing_site, r"utm_medium=([^&]+)") AS utm_medium,
    REGEXP_EXTRACT(landing_site, r"utm_campaign=([^&]+)") AS utm_campaign,
    REGEXP_EXTRACT(landing_site, r"utm_content=([^&]+)") AS utm_content,
    REGEXP_EXTRACT(landing_site, r"utm_term=([^&]+)") AS utm_term,
    REGEXP_EXTRACT(landing_site, r"utm_id=([^&]+)") AS utm_id,
    REGEXP_EXTRACT(landing_site, r"campaign_id=([^&]+)") AS campaign_id,
    REGEXP_EXTRACT(landing_site, r"ad_id=([^&]+)") AS ad_id,
    admin_graphql_api_id,

    ARRAY_TO_STRING(
    ARRAY(
      SELECT JSON_EXTRACT_SCALAR(element, '$.title') 
      FROM UNNEST(JSON_EXTRACT_ARRAY(discount_applications)) AS element
    ), ',') AS discount_application,

    payment_gateway_names

  FROM `shopify-pubsub-project.pilgrim_bi_airbyte.orders`

)

SELECT 
  _airbyte_extracted_at, Order_note, Order_cancelled_reason, Order_cancelled_at, landing_weburl,
  referring_weburl, Order_cart_token, browser_ip_id, Order_checkout_id, Order_checkout_token, 
  Order_closed_at, Order_fulfillment_status, Order_currency, Order_shop_url, Order_confirmed, 
  Order_estimated_taxes, isTestOrder, Order_taxes_included, Order_financial_status, 
  Order_source_name, Order_app_id, total_line_items_price, total_tax, Order_total_price, 
  Order_total_discounts, Order_tags, Order_updated_at, Order_created_at, Order_processed_at, 
  Order_name, Order_id, Order_token, Order_number, customer_id, tax_channel_liable, 
  tax_price, tax_rate, tax_title, browser_language, browser_height, browser_ip, 
  browser_width, session_hash, browser_agent, discount_amount, discount_type, 
  billing_address, billing_city, billing_country, billing_country_code, billing_province, 
  billing_province_code, billing_zip, shipping_address, shipping_city, shipping_country, 
  shipping_country_code, shipping_province, shipping_province_code, shipping_zip, 
  admin_graphql_api_id,utm_source,utm_medium,utm_campaign,utm_content,utm_term,utm_id,campaign_id,ad_id,
  discount_application,
  discount_code,
  CONCAT(coalesce(discount_application,''), ' - ', coalesce(discount_code,'')) as discount_final,
  STRING_AGG(payment_gateway_name, ', ') AS payment_gateway_names,
  

FROM source_orders
LEFT JOIN UNNEST(JSON_VALUE_ARRAY(payment_gateway_names)) AS payment_gateway_name
GROUP BY ALL
