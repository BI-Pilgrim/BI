
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_product_variants`
PARTITION BY DATE_TRUNC(metafield_variant_created_at, DAY)
CLUSTER BY metafield_variant_id
OPTIONS(
  description = "Metafield Products variants table is partitioned on variant created at",
  require_partition_filter = FALSE
)
 AS 
SELECT  
distinct
_airbyte_extracted_at,
id as metafield_variant_id,
key,
type,
value,
owner_id,
shop_url,
namespace,
created_at as metafield_variant_created_at,
updated_at as metafield_variant_updated_at,
value_type,
description,
owner_resource,
admin_graphql_api_id
FROM `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_product_variants`
