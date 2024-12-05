
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_orders` AS target  
USING (
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

WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.Metafield_order_id = source.Metafield_order_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Metafield_order_id = source.Metafield_order_id,
target.Metafield_order_key = source.Metafield_order_key,
target.type = source.type,
target.value = source.value,
target.owner_id = source.owner_id,
target.shop_url = source.shop_url,
target.namespace = source.namespace,
target.Order_created_at = source.Order_created_at,
target.Order_updated_at = source.Order_updated_at,
target.owner_resource = source.owner_resource,
target.admin_graphql_api_id = source.admin_graphql_api_id

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Metafield_order_id,
Metafield_order_key,
type,
value,
owner_id,
shop_url,
namespace,
Order_created_at,
Order_updated_at,
owner_resource,
admin_graphql_api_id

    
  )
  VALUES (
source._airbyte_extracted_at,
source.Metafield_order_id,
source.Metafield_order_key,
source.type,
source.value,
source.owner_id,
source.shop_url,
source.namespace,
source.Order_created_at,
source.Order_updated_at,
source.owner_resource,
source.admin_graphql_api_id


  );
