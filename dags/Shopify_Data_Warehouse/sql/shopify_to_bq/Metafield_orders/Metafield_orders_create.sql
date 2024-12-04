
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_orders`
PARTITION BY DATE_TRUNC(Order_created_at,day)
CLUSTER BY Metafield_order_id
OPTIONS(
 description = "Metafield orders table is partitioned on metafield order created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id as Metafield_order_id,
key as Metafield_order_key,
type,
value,
owner_id,
shop_url,
namespace,
created_at as Order_created_at,
updated_at as Order_updated_at,
owner_resource,
admin_graphql_api_id
FROM
`shopify-pubsub-project.pilgrim_bi_airbyte.metafield_orders`
