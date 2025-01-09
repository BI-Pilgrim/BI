
MERGE INTO  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Discount_Code` AS target

USING (
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

  WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.Discount_code_id = source.Discount_code_id
and target.Discount_code = source.Discount_code

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Discount_code_id=source.Discount_code_id,
target.Discount_code=source.Discount_code,
target.Discount_title=source.Discount_title,
target.Discount_status=source.Discount_status,
target.Discount_code_ends_at=source.Discount_code_ends_at,
target.Discount_summary=source.Discount_summary,
target.shop_url=source.shop_url,
target.typename=source.typename,
target.Discount_code_starts_at=source.Discount_code_starts_at,
target.Discount_code_created_at=source.Discount_code_created_at,
target.Discount_code_updated_at=source.Discount_code_updated_at,
target.Discount_code_count=source.Discount_code_count,
target.Discount_sales_amount=source.Discount_sales_amount,
target.Discount_usage_count=source.Discount_usage_count,
target.Discount_usage_limit=source.Discount_usage_limit,
target.Discount_type=source.Discount_type,
target.price_rule_id=source.price_rule_id,
target.async_usage_count=source.async_usage_count,
target.admin_graphql_api_id=source.admin_graphql_api_id,
target.applies_once_per_customer = source.applies_once_per_customer
  

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Discount_code_id,
Discount_code,
Discount_title,
Discount_status,
Discount_code_ends_at,
Discount_summary,
shop_url,
typename,
Discount_code_starts_at,
Discount_code_created_at,
Discount_code_updated_at,
Discount_code_count,
Discount_sales_amount,
Discount_usage_count,
Discount_usage_limit,
Discount_type,
price_rule_id,
async_usage_count,
admin_graphql_api_id,
applies_once_per_customer

) VALUES (
  source._airbyte_extracted_at,
source.Discount_code_id,
source.Discount_code,
source.Discount_title,
source.Discount_status,
source.Discount_code_ends_at,
source.Discount_summary,
source.shop_url,
source.typename,
source.Discount_code_starts_at,
source.Discount_code_created_at,
source.Discount_code_updated_at,
source.Discount_code_count,
source.Discount_sales_amount,
source.Discount_usage_count,
source.Discount_usage_limit,
source.Discount_type,
source.price_rule_id,
source.async_usage_count,
source.admin_graphql_api_id,
source.applies_once_per_customer
);
