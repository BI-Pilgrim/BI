

CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Abandoned_checkout`
PARTITION BY DATE_TRUNC(aband_created_at,day)
-- CLUSTER BY fulfillment_shipment_status
OPTIONS(
 description = "Abandoned checkout table is partitioned on abandoned checkout created at",
 require_partition_filter = False
 )
 AS 
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


FROM  `shopify-pubsub-project.airbyte711.abandoned_checkouts`
group by All
