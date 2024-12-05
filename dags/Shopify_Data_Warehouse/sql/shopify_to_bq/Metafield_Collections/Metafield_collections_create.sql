
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_collections`
PARTITION BY DATE_TRUNC(metafield_collection_created_at,day)
CLUSTER BY metafield_collection_id
OPTIONS(
 description = "Metafield Collection table is partitioned on Metafield collection created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id as metafield_collection_id,
key as metafield_collection_key,
type as collection_type,
value collection_value,
owner_id,
shop_url,
namespace,
created_at as metafield_collection_created_at,
updated_at as metafield_collection_updated_at,
description,
owner_resource,
admin_graphql_api_id

from `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_collections`
