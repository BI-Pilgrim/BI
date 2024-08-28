
MERGE INTO `shopify-pubsub-project.Shopify_staging.Metafield_customers` AS target

USING (
  SELECT
    _airbyte_extracted_at as _airbyte_extracted_at,
    id as metafield_id,
    key as metafield_key,
    type as metafield_type,
    value as metafield_value,
    owner_id as customer_id,
    shop_url as metafield_shop_url,
    namespace as metafield_namespace,
    created_at as customer_created_at,
    updated_at as customer_updated_at,
    owner_resource as metafield_owner_resource

  FROM `shopify-pubsub-project.airbyte711.metafield_customers`
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 1 DAY)
 
 ) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
    target._airbyte_extracted_at = source._airbyte_extracted_at,
    target.metafield_id = source.metafield_id,
    target.metafield_key = source.metafield_key,
    target.metafield_type = source.metafield_type,
    target.metafield_value = source.metafield_value,
    target.customer_id = source.customer_id,
    target.metafield_shop_url = source.metafield_shop_url,
    target.metafield_namespace = source.metafield_namespace,
    target.customer_created_at = source.customer_created_at,
    target.customer_updated_at = source.customer_updated_at,
    target.metafield_owner_resource = source.metafield_owner_resource
WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
metafield_id,
metafield_key,
metafield_type,
metafield_value,
customer_id,
metafield_shop_url,
metafield_namespace,
customer_created_at,
customer_updated_at,
metafield_owner_resource
   )
  VALUES (
source._airbyte_extracted_at,
source.metafield_id,
source.metafield_key,
source.metafield_type,
source.metafield_value,
source.customer_id,
source.metafield_shop_url,
source.metafield_namespace,
source.customer_created_at,
source.customer_updated_at,
metafield_owner_resource
  )
