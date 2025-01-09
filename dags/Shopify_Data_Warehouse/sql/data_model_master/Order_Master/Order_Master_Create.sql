CREATE or replace TABLE `shopify-pubsub-project.Shopify_Production.Order_Master`
PARTITION BY DATE_TRUNC(Order_processed_at,day)
 
OPTIONS(
 description = "Order Master table is partitioned on Orderprocessed at day level",
 require_partition_filter = False
 )
 AS

SELECT
    distinct
    O._airbyte_extracted_at,
    O.Order_note,
    O.Order_cancelled_reason,
    O.Order_cancelled_at,
    O.landing_weburl,
    O.referring_weburl,
    O.Order_cart_token,
    O.browser_ip_id,
    O.Order_checkout_id,
    O.Order_checkout_token,
    O.Order_closed_at,
    O.Order_fulfillment_status,
    O.Order_currency,
    O.Order_shop_url,
    O.Order_confirmed,
    O.Order_estimated_taxes,
    O.isTestOrder,
    O.Order_taxes_included,
    O.Order_financial_status,
    O.Order_source_name,
    O.Order_app_id,
    O.total_line_items_price,
    O.total_tax,
    O.Order_total_price,
    O.Order_total_discounts,
    O.Order_tags,
    O.Order_updated_at,
    O.Order_created_at,
    O.Order_processed_at,
    O.Order_name,
    O.Order_id,
    O.Order_token,
    O.Order_number,
    O.customer_id,
    O.tax_channel_liable,
    O.tax_price,
    O.tax_rate,
    O.tax_title,
    O.browser_language,
    O.browser_height,
    O.browser_ip,
    O.browser_width,
    O.session_hash,
    O.browser_agent,
    O.discount_amount,
    O.discount_code,
    O.discount_type,
    O.billing_address,
    O.billing_city,
    O.billing_country,
    O.billing_country_code,
    O.billing_province,
    O.billing_province_code,
    O.billing_zip,
    O.shipping_address,
    O.shipping_city,
    O.shipping_country,
    O.shipping_country_code,
    O.shipping_province,
    O.shipping_province_code,
    O.shipping_zip,
    O.payment_gateway_names,
    T.*,

  FROM
    `shopify-pubsub-project.Shopify_staging.Orders` AS O
    LEFT OUTER JOIN (select 
Trans_test,
Trans_kind,
Trans_status,
Trans_gateway,
Trans_amount,
Trans_created_at,
Trans_processed_at,
Trans_order_id,
Trans_id,
Trans_payment_id,
payment_avs_result_code,
payment_credit_card_bin,
payment_credit_card_company,
payment_credit_card_expiration_month,
payment_credit_card_expiration_year,
payment_credit_card_name,
payment_credit_card_number,
payment_credit_card_wallet,
payment_cvv_result_code
from (
  SELECT  
  *,
  row_number() over(partition by Trans_order_id order by Trans_processed_at desc) as ranking
  FROM `shopify-pubsub-project.Shopify_staging.Transactions` 

)
where ranking = 1 ) AS T 
ON O.Order_id = T.Trans_order_id;
