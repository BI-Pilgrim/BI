

CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Abandoned_checkout`
PARTITION BY DATE_TRUNC(aband_created_at,day)
-- CLUSTER BY fulfillment_shipment_status
OPTIONS(
 description = "Abandoned checkout table is partitioned on abandoned checkout created at",
 require_partition_filter = False
 )
 AS 
SELECT 
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
  id as abandoned_checkout_id,
  name as abandoned_checkout_name,
  token as abandoned_checkout_token,

JSON_EXTRACT(customer, '$.id') AS customer_id,


JSON_EXTRACT(discount_codes, '$[0].amount') AS disocunt_amount,
JSON_EXTRACT(discount_codes, '$[0].code') AS disocunt_code,
JSON_EXTRACT(discount_codes, '$[0].type') AS discount_type


FROM  `shopify-pubsub-project.airbyte711.abandoned_checkouts`
