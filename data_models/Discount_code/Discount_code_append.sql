
MERGE INTO `shopify-pubsub-project.Shopify_staging.Discount_Code` AS target

USING (
  SELECT
    _airbyte_extracted_at as _airbyte_extracted_at,
    CAST(id AS STRING) as discount_id,
    code as disocunt_code,
    summary as discount_summary,
    shop_url as shop_url,
    created_at as discount_created_at,
    updated_at as discount_updated_at,
    usage_count as discount_usage_count,
    discount_type,
    CAST(price_rule_id AS STRING) AS price_rule_id,

  FROM `shopify-pubsub-project.airbyte711.discount_codes`
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 1 DAY)
 
 ) AS source
ON target.discount_id = source.discount_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.discount_id = source.discount_id,
target.disocunt_code = source.disocunt_code,
target.discount_summary = source.discount_summary,
target.shop_url = source.shop_url,
target.discount_created_at = source.discount_created_at,
target.discount_updated_at = source.discount_updated_at,
target.discount_usage_count = source.discount_usage_count,
target.discount_type = source.discount_type,
target.price_rule_id = source.price_rule_id

WHEN NOT MATCHED THEN INSERT (
  _airbyte_extracted_at,
  discount_id,
  disocunt_code,
  discount_summary,
  shop_url,
  discount_created_at,
  discount_updated_at,
  discount_usage_count,
  discount_type,
  price_rule_id

   )
  VALUES (
  _airbyte_extracted_at,
  discount_id,
  disocunt_code,
  discount_summary,
  shop_url,
  discount_created_at,
  discount_updated_at,
  discount_usage_count,
  discount_type,
  price_rule_id
  )




