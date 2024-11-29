
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Discount_Code`
PARTITION BY DATE_TRUNC(Discount_code_created_at,day)
CLUSTER BY Discount_status, Discount_code
OPTIONS(
 description = "Discount Code table is partitioned on Discount code created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id as Discount_code_id,
code as Discount_code,
title as Discount_title,
status as Discount_status,
ends_at as Discount_code_ends_at,
summary as Discount_summary,
shop_url,
typename,
starts_at as Discount_code_starts_at,
created_at as Discount_code_created_at,
updated_at as Discount_code_updated_at,
CAST(JSON_EXTRACT_SCALAR(codes_count,'$.count') as INT64) as Discount_code_count,
CAST(ROUND(CAST(JSON_EXTRACT_SCALAR(total_sales,'$.amount') as FLOAT64))AS INT64) as Discount_sales_amount,
usage_count as Discount_usage_count,
usage_limit as Discount_usage_limit,
discount_type as Discount_type,
price_rule_id as price_rule_id,
async_usage_count,
admin_graphql_api_id,
applies_once_per_customer
from `shopify-pubsub-project.pilgrim_bi_airbyte.discount_codes`
