
MERGE INTO `shopify-pubsub-project.Shopify_staging.Abandoned_checkout` AS target

USING (
  SELECT
  distinct
      _airbyte_extracted_at as _airbyte_extracted_at,
  source_name as aband_source_name,
  referring_site as aband_referring_site,
  total_line_items_price as aband_total_line_items_price,
  total_discounts as aband_total_discounts,
  total_price as aband_total_price,
  total_tax as aband_total_tax,
  completed_at as aband_completed_at,
  phone as aband_phone,
  landing_site as aband_landing_site,
  created_at as aband_created_at,
  updated_at as aband_updated_at,
  email as customer_email,
  cart_token as aband_cart_token,
  abandoned_checkout_url as abandoned_checkout_url,
  CAST(id AS STRING) as abandoned_checkout_id,
  name as abandoned_checkout_name,
  token as abandoned_checkout_token,

CAST(JSON_EXTRACT_SCALAR(customer, '$.id') AS STRING) AS customer_id,


CAST(JSON_EXTRACT_SCALAR(discount_codes, '$[0].amount') AS FLOAT64) AS disocunt_amount,
CAST(JSON_EXTRACT_SCALAR(discount_codes, '$[0].code') AS STRING) AS disocunt_code,
CAST(JSON_EXTRACT_SCALAR(discount_codes, '$[0].type') AS STRING) AS discount_type

  FROM `shopify-pubsub-project.airbyte711.abandoned_checkouts`
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.abandoned_checkout_id = source.abandoned_checkout_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.aband_source_name = source.aband_source_name,
target.aband_referring_site = source.aband_referring_site,
target.aband_total_line_items_price = source.aband_total_line_items_price,
target.aband_total_discounts = source.aband_total_discounts,
target.aband_total_price = source.aband_total_price,
target.aband_total_tax = source.aband_total_tax,
target.aband_completed_at = source.aband_completed_at,
target.aband_phone = source.aband_phone,
target.aband_landing_site = source.aband_landing_site,
target.aband_created_at = source.aband_created_at,
target.aband_updated_at = source.aband_updated_at,
target.customer_email = source.customer_email,
target.aband_cart_token = source.aband_cart_token,
target.abandoned_checkout_url = source.abandoned_checkout_url,
target.abandoned_checkout_id = source.abandoned_checkout_id,
target.abandoned_checkout_name = source.abandoned_checkout_name,
target.abandoned_checkout_token = source.abandoned_checkout_token,
target.customer_id = source.customer_id,
target.disocunt_amount = source.disocunt_amount,
target.disocunt_code = source.disocunt_code,
target.discount_type = source.discount_type

WHEN NOT MATCHED THEN INSERT (
 _airbyte_extracted_at,
aband_source_name,
aband_referring_site,
aband_total_line_items_price,
aband_total_discounts,
aband_total_price,
aband_total_tax,
aband_completed_at,
aband_phone,
aband_landing_site,
aband_created_at,
aband_updated_at,
customer_email,
aband_cart_token,
abandoned_checkout_url,
abandoned_checkout_id,
abandoned_checkout_name,
abandoned_checkout_token,
customer_id,
disocunt_amount,
disocunt_code,
discount_type

 )
  VALUES (
source._airbyte_extracted_at,
source.aband_source_name,
source.aband_referring_site,
source.aband_total_line_items_price,
source.aband_total_discounts,
source.aband_total_price,
source.aband_total_tax,
source.aband_completed_at,
source.aband_phone,
source.aband_landing_site,
source.aband_created_at,
source.aband_updated_at,
source.customer_email,
source.aband_cart_token,
source.abandoned_checkout_url,
source.abandoned_checkout_id,
source.abandoned_checkout_name,
source.abandoned_checkout_token,
source.customer_id,
source.disocunt_amount,
source.disocunt_code,
source.discount_type

  )

