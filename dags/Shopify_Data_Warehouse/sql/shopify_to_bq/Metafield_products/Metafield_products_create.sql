
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_products`
PARTITION BY DATE_TRUNC(metafield_product_created_at, DAY)
CLUSTER BY metafield_product_id
OPTIONS(
  description = "metfaield Products table is partitioned on metafield_product_created_at",
  require_partition_filter = FALSE
)
 AS 
SELECT  
distinct
_airbyte_extracted_at,
id as metafield_product_id,
key as metafield_key,
type,
value,
owner_id,
shop_url,
namespace,
created_at as metafield_product_created_at,
updated_at as metafield_product_updated_at,
description,
owner_resource,
admin_graphql_api_id
 FROM `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_products`
