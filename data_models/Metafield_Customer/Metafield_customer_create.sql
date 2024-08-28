
-- current_datetime("Asia/Kolkata"),

CREATE TABLE `shopify-pubsub-project.Shopify_staging.Metafield_customers`
PARTITION BY DATE_TRUNC(customer_created_at,day)
CLUSTER BY metafield_value
OPTIONS(
 description = "Metafield Customer table is partitioned on customer created at day level",
 require_partition_filter = FALSE
 )
 AS 
SELECT 
_airbyte_extracted_at,
id as metafield_id,
key as metafield_key,
type as metafield_type,
value as metafield_value,
owner_id as customer_id,
shop_url as metafield_shop_url,
namespace as metafield_namespace,
created_at as customer_created_at,
updated_at as customer_updated_at,
owner_resource as metafield_owner_resource,
 FROM  `shopify-pubsub-project.airbyte711.metafield_customers`
