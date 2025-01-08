
MERGE INTO  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Fulfillment_Orders` AS target

USING (
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

WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.fulfillment_id = source.fulfillment_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.fulfillment_order_id = source.fulfillment_order_id,
target.fulfillment_status = source.fulfillment_status,
target.shop_id = source.shop_id,
target.order_id = source.order_id,
target.shop_url = source.shop_url,
target.channel_id = source.channel_id,
target.fulfillment_order_created_at = source.fulfillment_order_created_at,
target.order_fulfilled_at = source.order_fulfilled_at,
target.order_fulfilled_by = source.order_fulfilled_by,
target.fulfillment_order_updated_at = source.fulfillment_order_updated_at,
target.destination_address = source.destination_address,
target.destination_city = source.destination_city,
target.destination_email = source.destination_email,
target.customer_name = source.customer_name,
target.customer_id = source.customer_id,
target.customer_phone = source.customer_phone,
target.destination_state = source.destination_state,
target.destination_zip   = source.destination_zip,
target.request_status = source.request_status,
target.delivery_id = source.delivery_id,
target.max_delivery_date_time = source.max_delivery_date_time,
target.delivery_method_type = source.delivery_method_type,
target.min_delivery_date_time = source.min_delivery_date_time,
target.assigned_location_address = source.assigned_location_address,
target.assigned_location_city = source.assigned_location_city,
target.assigned_location_name = source.assigned_location_name,
target.assigned_location_state = source.assigned_location_state,
target.assigned_location_zip = source.assigned_location_zip,
target.assigned_location_phone = source.assigned_location_phone,
target.fulfillable_quantity = source.fulfillable_quantity,
target.fulfillment_id = source.fulfillment_id,
target.inventory_item_id = source.inventory_item_id,
target.line_item_id = source.line_item_id,
target.variant_id = source.variant_id,
target.admin_graphql_api_id = source.admin_graphql_api_id,
target.assigned_location_id = source.assigned_location_id


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
fulfillment_order_id,
fulfillment_status,
shop_id,
order_id,
shop_url,
channel_id,
fulfillment_order_created_at,
order_fulfilled_at,
order_fulfilled_by,
fulfillment_order_updated_at,
destination_address,
destination_city,
destination_email,
customer_name,
customer_id,
customer_phone,
destination_state,
destination_zip  ,
request_status,
delivery_id,
max_delivery_date_time,
delivery_method_type,
min_delivery_date_time,
assigned_location_address,
assigned_location_city,
assigned_location_name,
assigned_location_state,
assigned_location_zip,
assigned_location_phone,
fulfillable_quantity,
fulfillment_id,
inventory_item_id,
line_item_id,
variant_id,
admin_graphql_api_id,
assigned_location_id

 )

  VALUES (
source._airbyte_extracted_at,
source.fulfillment_order_id,
source.fulfillment_status,
source.shop_id,
source.order_id,
source.shop_url,
source.channel_id,
source.fulfillment_order_created_at,
source.order_fulfilled_at,
source.order_fulfilled_by,
source.fulfillment_order_updated_at,
source.destination_address,
source.destination_city,
source.destination_email,
source.customer_name,
source.customer_id,
source.customer_phone,
source.destination_state,
source.destination_zip  ,
source.request_status,
source.delivery_id,
source.max_delivery_date_time,
source.delivery_method_type,
source.min_delivery_date_time,
source.assigned_location_address,
source.assigned_location_city,
source.assigned_location_name,
source.assigned_location_state,
source.assigned_location_zip,
source.assigned_location_phone,
source.fulfillable_quantity,
source.fulfillment_id,
source.inventory_item_id,
source.line_item_id,
source.variant_id,
source.admin_graphql_api_id,
source.assigned_location_id
  )
