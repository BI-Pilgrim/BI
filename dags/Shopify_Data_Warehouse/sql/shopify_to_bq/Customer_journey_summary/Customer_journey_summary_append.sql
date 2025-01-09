
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customer_journey_summary` AS target

USING (
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
CAST(JSON_EXTRACT_SCALAR(customer_journey_summary, "$.moments_count.count") AS INT64) AS moments_count,
  JSON_EXTRACT_SCALAR(customer_journey_summary, "$.moments_count.precision") AS moments_count_precision,
  admin_graphql_api_id
FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte.customer_journey_summary`


  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.order_id = source.order_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.order_id = source.order_id,
target.shop_url = source.shop_url,
target.Customer_journey_created_at = source.Customer_journey_created_at,
target.Customer_journey_updated_at = source.Customer_journey_updated_at,
target.customer_order_index = source.customer_order_index,
target.days_to_conversion = source.days_to_conversion,
target.first_visit_landing = source.first_visit_landing,
target.first_visit_referral = source.first_visit_referral,
target.first_visit_source = source.first_visit_source,
target.first_visit_source_type = source.first_visit_source_type,
target.last_visit_landing = source.last_visit_landing,
target.last_visit_referral = source.last_visit_referral,
target.last_visit_source = source.last_visit_source,
target.last_visit_source_type = source.last_visit_source_type,
target.moments_count = source.moments_count,
target.moments_count_precision = source.moments_count_precision,
target.admin_graphql_api_id = source.admin_graphql_api_id


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
order_id,
shop_url,
Customer_journey_created_at,
Customer_journey_updated_at,
customer_order_index,
days_to_conversion,
first_visit_landing,
first_visit_referral,
first_visit_source,
first_visit_source_type,
last_visit_landing,
last_visit_referral,
last_visit_source,
last_visit_source_type,
moments_count,
moments_count_precision,
admin_graphql_api_id
   )

  VALUES (
source._airbyte_extracted_at,
source.order_id,
source.shop_url,
source.Customer_journey_created_at,
source.Customer_journey_updated_at,
source.customer_order_index,
source.days_to_conversion,
source.first_visit_landing,
source.first_visit_referral,
source.first_visit_source,
source.first_visit_source_type,
source.last_visit_landing,
source.last_visit_referral,
source.last_visit_source,
source.last_visit_source_type,
source.moments_count,
source.moments_count_precision,
source.admin_graphql_api_id

  )
