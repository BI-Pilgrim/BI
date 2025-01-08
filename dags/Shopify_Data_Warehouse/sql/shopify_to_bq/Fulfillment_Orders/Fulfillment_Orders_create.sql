
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Fulfillment_Orders`
PARTITION BY DATE_TRUNC(fulfillment_order_created_at, DAY)
CLUSTER BY fulfillment_order_id
OPTIONS(
  description = "fulfillment orders table is partitioned on order created at",
  require_partition_filter = FALSE
)
 AS 
SELECT  
distinct
_airbyte_extracted_at,
id as fulfillment_order_id,
status as fulfillment_status,
shop_id,
order_id,
shop_url,
channel_id,
created_at as fulfillment_order_created_at,
fulfill_at as order_fulfilled_at,
fulfill_by as order_fulfilled_by,
updated_at as fulfillment_order_updated_at,
CONCAT(
    JSON_EXTRACT_SCALAR(destination, '$.address1'), ', ',
    JSON_EXTRACT_SCALAR(destination, '$.address2')
  ) AS destination_address,
  JSON_EXTRACT_SCALAR(destination, '$.city') AS destination_city,
  JSON_EXTRACT_SCALAR(destination, '$.email') AS destination_email,
  CONCAT(
    JSON_EXTRACT_SCALAR(destination, '$.first_name'), ' ',
    JSON_EXTRACT_SCALAR(destination, '$.last_name')
  ) AS customer_name,
  CAST(JSON_EXTRACT_SCALAR(destination, '$.id') AS INT64) AS customer_id,
  JSON_EXTRACT_SCALAR(destination, '$.phone') AS customer_phone,
  JSON_EXTRACT_SCALAR(destination, '$.province') AS destination_state,
  JSON_EXTRACT_SCALAR(destination, '$.zip') AS destination_zip  ,
request_status,
JSON_EXTRACT_SCALAR(delivery_method, '$.id') AS delivery_id,
JSON_EXTRACT_SCALAR(delivery_method, '$.max_delivery_date_time') AS max_delivery_date_time,
JSON_EXTRACT_SCALAR(delivery_method, '$.method_type') AS delivery_method_type,
JSON_EXTRACT_SCALAR(delivery_method, '$.min_delivery_date_time') AS min_delivery_date_time,

CONCAT(
    JSON_EXTRACT_SCALAR(assigned_location, '$.address1'), ', ',
    JSON_EXTRACT_SCALAR(assigned_location, '$.address2')
  ) AS assigned_location_address,

JSON_EXTRACT_SCALAR(assigned_location, '$.city') AS assigned_location_city,
JSON_EXTRACT_SCALAR(assigned_location,'$.name') as assigned_location_name,
JSON_EXTRACT_SCALAR(assigned_location,'$.province') as assigned_location_state,
JSON_EXTRACT_SCALAR(assigned_location,'$.zip') as assigned_location_zip,
JSON_EXTRACT_SCALAR(assigned_location,'$.phone') as assigned_location_phone,

CAST(JSON_EXTRACT_SCALAR(item, '$.fulfillable_quantity') AS INT64) AS fulfillable_quantity,
CAST(JSON_EXTRACT_SCALAR(item, '$.id') AS INT64) AS fulfillment_id,
CAST(JSON_EXTRACT_SCALAR(item, '$.inventory_item_id') AS INT64) AS inventory_item_id,
CAST(JSON_EXTRACT_SCALAR(item, '$.line_item_id') AS INT64) AS line_item_id,
CAST(JSON_EXTRACT_SCALAR(item, '$.variant_id') AS INT64) AS variant_id,

admin_graphql_api_id,
assigned_location_id
FROM
`shopify-pubsub-project.pilgrim_bi_airbyte.fulfillment_orders`,
UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS item 
