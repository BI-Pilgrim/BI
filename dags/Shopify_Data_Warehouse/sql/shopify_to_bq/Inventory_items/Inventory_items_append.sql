
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Inventory_items` AS target  
USING (
SELECT 
distinct
_airbyte_extracted_at,
id as Inventory_item_id,
sku as Item_sku,
cost as Item_cost,
tracked,
shop_url,
created_at as Inventory_item_created_at,
updated_at as Inventory_item_updated_at,
currency_code,
requires_shipping,
duplicate_sku_count,
admin_graphql_api_id,
country_code_of_origin
from `shopify-pubsub-project.pilgrim_bi_airbyte.inventory_items`

WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.Inventory_item_id = source.Inventory_item_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Inventory_item_id=source.Inventory_item_id,
target.Item_sku=source.Item_sku,
target.Item_cost=source.Item_cost,
target.tracked=source.tracked,
target.shop_url=source.shop_url,
target.Inventory_item_created_at=source.Inventory_item_created_at,
target.Inventory_item_updated_at=source.Inventory_item_updated_at,
target.currency_code=source.currency_code,
target.requires_shipping=source.requires_shipping,
target.duplicate_sku_count=source.duplicate_sku_count,
target.admin_graphql_api_id=source.admin_graphql_api_id,
target.country_code_of_origin=source.country_code_of_origin

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Inventory_item_id,
Item_sku,
Item_cost,
tracked,
shop_url,
Inventory_item_created_at,
Inventory_item_updated_at,
currency_code,
requires_shipping,
duplicate_sku_count,
admin_graphql_api_id,
country_code_of_origin
    
  )
  VALUES (
source._airbyte_extracted_at,
source.Inventory_item_id,
source.Item_sku,
source.Item_cost,
source.tracked,
source.shop_url,
source.Inventory_item_created_at,
source.Inventory_item_updated_at,
source.currency_code,
source.requires_shipping,
source.duplicate_sku_count,
source.admin_graphql_api_id,
source.country_code_of_origin
  );
