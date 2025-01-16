
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Inventory_level`
PARTITION BY DATE_TRUNC(inventory_created_at, day)
CLUSTER BY inventory_level_id
OPTIONS(
  description = "Inventory level table is partitioned on inventory_created_at",
  require_partition_filter = False
)
AS 
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
  UNNEST(JSON_EXTRACT_ARRAY(quantities)) AS item;
