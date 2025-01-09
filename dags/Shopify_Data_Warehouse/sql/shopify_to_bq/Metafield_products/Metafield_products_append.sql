

MERGE INTO  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_products` AS target

USING (
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


  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.metafield_product_id = source.metafield_product_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.metafield_product_id = source.metafield_product_id,
target.metafield_key = source.metafield_key,
target.type = source.type,
target.value = source.value,
target.owner_id = source.owner_id,
target.shop_url = source.shop_url,
target.namespace = source.namespace,
target.metafield_product_created_at = source.metafield_product_created_at,
target.metafield_product_updated_at = source.metafield_product_updated_at,
target.description = source.description,
target.owner_resource = source.owner_resource,
target.admin_graphql_api_id = source.admin_graphql_api_id


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
metafield_product_id,
metafield_key,
type,
value,
owner_id,
shop_url,
namespace,
metafield_product_created_at,
metafield_product_updated_at,
description,
owner_resource,
admin_graphql_api_id
 )

  VALUES (
source._airbyte_extracted_at,
source.metafield_product_id,
source.metafield_key,
source.type,
source.value,
source.owner_id,
source.shop_url,
source.namespace,
source.metafield_product_created_at,
source.metafield_product_updated_at,
source.description,
source.owner_resource,
source.admin_graphql_api_id
  )
