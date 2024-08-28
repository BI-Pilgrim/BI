
-- current_datetime("Asia/Kolkata"),

CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Fulfillments`
PARTITION BY DATE_TRUNC(fulfillment_created_at,day)
CLUSTER BY fulfillment_shipment_status
OPTIONS(
 description = "Fulfillment table is partitioned on ",
 require_partition_filter = False
 )
 AS 
SELECT 
_airbyte_extracted_at as _airbyte_extracted_at,
shop_url as fulfillment_shop_url,
status as fulfillment_status,
service as fulfillment_service,
location_id as fulfillment_location_id,
shipment_status as fulfillment_shipment_status,
tracking_company as fulfillment_tracking_company,
updated_at as fulfillment_updated_at,
tracking_number as fulfillment_tracking_number,
tracking_url as fulfillment_tracking_url,
created_at as fulfillment_created_at,
order_id as fulfillment_order_id,
name as fulfillment_name,
id as fulfillment_id,
tracking_urls[0] as fulfillment_tracking_urls,
tracking_numbers[0] as fulfillment_tracking_numbers,
FROM  `shopify-pubsub-project.airbyte711.fulfillments`
