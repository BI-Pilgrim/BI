
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Inventory_items`
PARTITION BY DATE_TRUNC(Inventory_item_created_at,day)
CLUSTER BY Inventory_item_id
OPTIONS(
 description = "Inventory items table is partitioned on Inventory item created at ",
 require_partition_filter = False
 )
 AS 
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
