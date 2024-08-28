
MERGE INTO `shopify-pubsub-project.Shopify_staging.Fulfillments` AS target

USING (
  SELECT
    _airbyte_extracted_at as _airbyte_extracted_at,
    shop_url as fulfillment_shop_url,
    status as fulfillment_status,A
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
  FROM `shopify-pubsub-project.airbyte711.fulfillments`
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 1 DAY)
 
 ) AS source
ON target.fulfillment_order_id = source.fulfillment_order_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
  target._airbyte_extracted_at = source._airbyte_extracted_at,
  target.fulfillment_shop_url = source.fulfillment_shop_url,
  target.fulfillment_status = source.fulfillment_status,
  target.fulfillment_service = source.fulfillment_service,
  target.fulfillment_location_id = source.fulfillment_location_id,
  target.fulfillment_shipment_status = source.fulfillment_shipment_status,
  target.fulfillment_tracking_company = source.fulfillment_tracking_company,
  target.fulfillment_updated_at = source.fulfillment_updated_at,
  target.fulfillment_tracking_number = source.fulfillment_tracking_number,
  target.fulfillment_tracking_url = source.fulfillment_tracking_url,
  target.fulfillment_created_at = source.fulfillment_created_at,
  target.fulfillment_order_id = source.fulfillment_order_id,
  target.fulfillment_name = source.fulfillment_name,
  target.fulfillment_id = source.fulfillment_id,
  target.fulfillment_tracking_urls = source.fulfillment_tracking_urls,
  target.fulfillment_tracking_numbers = source.fulfillment_tracking_numbers

WHEN NOT MATCHED THEN INSERT (
  _airbyte_extracted_at,
fulfillment_shop_url,
fulfillment_status,
fulfillment_service,
fulfillment_location_id,
fulfillment_shipment_status,
fulfillment_tracking_company,
fulfillment_updated_at,
fulfillment_tracking_number,
fulfillment_tracking_url,
fulfillment_created_at,
fulfillment_order_id,
fulfillment_name,
fulfillment_id,
fulfillment_tracking_urls,
fulfillment_tracking_numbers

  )
  VALUES (
source._airbyte_extracted_at,
source.fulfillment_shop_url,
source.fulfillment_status,
source.fulfillment_service,
source.fulfillment_location_id,
source.fulfillment_shipment_status,
source.fulfillment_tracking_company,
source.fulfillment_updated_at,
source.fulfillment_tracking_number,
source.fulfillment_tracking_url,
source.fulfillment_created_at,
source.fulfillment_order_id,
source.fulfillment_name,
source.fulfillment_id,
source.fulfillment_tracking_urls,
source.fulfillment_tracking_numbers

  )
