
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Inventory_level` AS target  
USING (
SELECT 
  DISTINCT
  _airbyte_extracted_at,
  id AS inventory_level_id,
  shop_url,
  created_at AS inventory_created_at,
  updated_at AS inventory_updated_at,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(item, '$.admin_graphql_api_id'), r'inventory_item_id=([0-9]+)') AS item_id,
  JSON_EXTRACT_SCALAR(item, '$.name') AS inventory_status,
  CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS INT64) AS inventory_quantity,
  CAST(JSON_EXTRACT_SCALAR(item, '$.updatedAt') AS TIMESTAMP) AS status_updated_at,
  location_id AS inventory_location_id,
  can_deactivate,
  CAST(JSON_EXTRACT_SCALAR(locations_count, "$.count") AS INT64) AS inventory_location_count,
  inventory_item_id,
  deactivation_alert,
  admin_graphql_api_id,
  inventory_history_url,

  -- Composite key for uniqueness
  CONCAT(
    CAST(id AS STRING), '-', 
    COALESCE(CAST(REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(item, '$.admin_graphql_api_id'), r'inventory_item_id=([0-9]+)') AS STRING), 'NULL'), '-', 
    CAST(location_id AS STRING), '-', 
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_at)
  ) AS composite_key

FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte.inventory_levels`,
  UNNEST(JSON_EXTRACT_ARRAY(quantities)) AS item

WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.composite_key = source.composite_key

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.inventory_level_id = source.inventory_level_id,
target.shop_url = source.shop_url,
target.inventory_created_at = source.inventory_created_at,
target.inventory_updated_at = source.inventory_updated_at,
target.item_id = source.item_id,
target.inventory_status = source.inventory_status,
target.inventory_quantity = source.inventory_quantity,
target.status_updated_at = source.status_updated_at,
target.inventory_location_id = source.inventory_location_id,
target.can_deactivate = source.can_deactivate,
target.inventory_location_count = source.inventory_location_count,
target.inventory_item_id = source.inventory_item_id,
target.deactivation_alert = source.deactivation_alert,
target.admin_graphql_api_id = source.admin_graphql_api_id,
target.inventory_history_url = source.inventory_history_url,
target.composite_key = source.composite_key


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
inventory_level_id,
shop_url,
inventory_created_at,
inventory_updated_at,
item_id,
inventory_status,
inventory_quantity,
status_updated_at,
inventory_location_id,
can_deactivate,
inventory_location_count,
inventory_item_id,
deactivation_alert,
admin_graphql_api_id,
inventory_history_url,
composite_key
  
  )
  VALUES (
  source._airbyte_extracted_at,
  source.inventory_level_id,
  source.shop_url,
  source.inventory_created_at,
  source.inventory_updated_at,
  source.item_id,
  source.inventory_status,
  source.inventory_quantity,
  source.status_updated_at,
  source.inventory_location_id,
  source.can_deactivate,
  source.inventory_location_count,
  source.inventory_item_id,
  source.deactivation_alert,
  source.admin_graphql_api_id,
  source.inventory_history_url,
  source.composite_key
  );
