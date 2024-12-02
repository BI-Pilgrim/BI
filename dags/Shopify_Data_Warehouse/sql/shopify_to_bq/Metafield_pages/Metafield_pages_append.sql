
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_pages` AS target  
USING (
  SELECT
    SELECT 
distinct
_airbyte_extracted_at,
id as Metafield_page_id,
key as Metafield_page_key,
type as Page_type,
value as Page_value,
owner_id,
shop_url,
namespace,
created_at as Metafield_page_created_at,
updated_at as Metafield_page_updated_at,
description as Page_description,
owner_resource,
admin_graphql_api_id
) AS source
  
ON target.Metafield_page_id = source.Metafield_page_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Metafield_page_id=source.Metafield_page_id,
target.Metafield_page_key=source.Metafield_page_key,
target.Page_type=source.Page_type,
target.Page_value=source.Page_value,
target.owner_id=source.owner_id,
target.shop_url=source.shop_url,
target.namespace=source.namespace,
target.Metafield_page_created_at=source.Metafield_page_created_at,
target.Metafield_page_updated_at=source.Metafield_page_updated_at,
target.Page_description=source.Page_description,
target.owner_resource=source.owner_resource,
target.admin_graphql_api_id=source.admin_graphql_api_id

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Metafield_page_id,
Metafield_page_key,
Page_type,
Page_value,
owner_id,
shop_url,
namespace,
Metafield_page_created_at,
Metafield_page_updated_at,
Page_description,
owner_resource,
admin_graphql_api_id
    
  )
  VALUES (
source.Metafield_page_id,
source.Metafield_page_key,
source.Page_type,
source.Page_value,
source.owner_id,
source.shop_url,
source.namespace,
source.Metafield_page_created_at,
source.Metafield_page_updated_at,
source.Page_description,
source.owner_resource,
source.admin_graphql_api_id
  );
