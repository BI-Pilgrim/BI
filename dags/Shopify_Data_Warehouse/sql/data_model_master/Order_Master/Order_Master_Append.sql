
MERGE INTO `shopify-pubsub-project.Shopify_Production.Order_Master` AS target

USING (
  
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

ON O.Order_id = T.Trans_order_id

 
  WHERE date(O._airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
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
target.Trans_test = source.Trans_test,
target.Trans_kind = source.Trans_kind,
target.Trans_status = source.Trans_status,
target.Trans_gateway = source.Trans_gateway,
target.Trans_amount = source.Trans_amount,
target.Trans_created_at = source.Trans_created_at,
target.Trans_processed_at = source.Trans_processed_at,
target.Trans_order_id = source.Trans_order_id,
target.Trans_id = source.Trans_id,
target.Trans_payment_id = source.Trans_payment_id,
target.payment_avs_result_code = source.payment_avs_result_code,
target.payment_credit_card_bin = source.payment_credit_card_bin,
target.payment_credit_card_company = source.payment_credit_card_company,
target.payment_credit_card_expiration_month = source.payment_credit_card_expiration_month,
target.payment_credit_card_expiration_year = source.payment_credit_card_expiration_year,
target.payment_credit_card_name = source.payment_credit_card_name,
target.payment_credit_card_number = source.payment_credit_card_number,
target.payment_credit_card_wallet = source.payment_credit_card_wallet,
target.payment_cvv_result_code = source.payment_cvv_result_code


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
source.payment_gateway_names,
source.Trans_test,
source.Trans_kind,
source.Trans_status,
source.Trans_gateway,
source.Trans_amount,
source.Trans_created_at,
source.Trans_processed_at,
source.Trans_order_id,
source.Trans_id,
source.Trans_payment_id,
source.payment_avs_result_code,
source.payment_credit_card_bin,
source.payment_credit_card_company,
source.payment_credit_card_expiration_month,
source.payment_credit_card_expiration_year,
source.payment_credit_card_name,
source.payment_credit_card_number,
source.payment_credit_card_wallet,
source.payment_cvv_result_code

  )

