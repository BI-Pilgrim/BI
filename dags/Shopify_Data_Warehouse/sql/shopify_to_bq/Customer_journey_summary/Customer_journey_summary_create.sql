

CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customer_journey_summary`
PARTITION BY DATE_TRUNC(Customer_journey_created_at,day)
CLUSTER BY order_id
OPTIONS(
 description = "Customer journey summary table is partitioned on  Customer journey created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
order_id,
shop_url,
created_at as Customer_journey_created_at,
updated_at as Customer_journey_updated_at,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.customer_order_index")as INT64) AS customer_order_index,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.days_to_conversion")as INT64) AS days_to_conversion,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.first_visit.landing_page")AS STRING) AS first_visit_landing,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.first_visit.referral_code") AS STRING) AS first_visit_referral,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.first_visit.source") AS STRING) AS first_visit_source,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.first_visit.source_type") AS STRING) AS first_visit_source_type,
  CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.last_visit.landing_page")AS STRING) AS last_visit_landing,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.first_visit.referral_code") AS STRING) AS last_visit_referral,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.first_visit.source") AS STRING) AS last_visit_source,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.first_visit.source_type") AS STRING) AS last_visit_source_type,
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.moments_count.count") AS INT64) AS moments_count_count,
  JSON_EXTRACT_SCALAR(customer_journey_summary, "$.moments_count.precision") AS moments_count_precision,
  admin_graphql_api_id
FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte.customer_journey_summary`
