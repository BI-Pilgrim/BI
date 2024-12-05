
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_Articles` AS target  
USING (
SELECT 
distinct
_airbyte_extracted_at,
id as Metafield_article_id,
key as Metafield_article_key,
type as Metafield_type,
value as Metafield_value,
owner_id,
shop_url,
namespace,
created_at as Metafield_article_created_at,
updated_at as Metafield_article_updated_at,
owner_resource,
admin_graphql_api_id

FROM
`shopify-pubsub-project.pilgrim_bi_airbyte.metafield_articles`


WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.Metafield_article_id = source.Metafield_article_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Metafield_article_id = source.Metafield_article_id,
target.Metafield_article_key = source.Metafield_article_key,
target.Metafield_type = source.Metafield_type,
target.Metafield_value = source.Metafield_value,
target.owner_id = source.owner_id,
target.shop_url = source.shop_url,
target.namespace = source.namespace,
target.Metafield_article_created_at = source.Metafield_article_created_at,
target.Metafield_article_updated_at = source.Metafield_article_updated_at,
target.owner_resource = source.owner_resource,
target.admin_graphql_api_id = source.admin_graphql_api_id



WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Metafield_article_id,
Metafield_article_key,
Metafield_type,
Metafield_value,
owner_id,
shop_url,
namespace,
Metafield_article_created_at,
Metafield_article_updated_at,
owner_resource,
admin_graphql_api_id

    
  )
  VALUES (
source._airbyte_extracted_at,
source.Metafield_article_id,
source.Metafield_article_key,
source.Metafield_type,
source.Metafield_value,
source.owner_id,
source.shop_url,
source.namespace,
source.Metafield_article_created_at,
source.Metafield_article_updated_at,
source.owner_resource,
source.admin_graphql_api_id


  );
