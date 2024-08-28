
CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Discount_Code`
PARTITION BY DATE_TRUNC(discount_created_at,day)
CLUSTER BY disocunt_code
OPTIONS(
 description = "Discount code table is partitioned on discount created at day level",
 require_partition_filter = False
 )
 AS 
SELECT 
_airbyte_extracted_at as _airbyte_extracted_at,
id as discount_id,
code as disocunt_code,
summary as discount_summary,
shop_url as shop_url,
created_at as discount_created_at,
updated_at as discount_updated_at,
usage_count as discount_usage_count,
discount_type,
price_rule_id,
 FROM  `shopify-pubsub-project.airbyte711.discount_codes`
