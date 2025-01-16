
MERGE INTO  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Fulfillments` AS target

USING (
SELECT  
  _airbyte_extracted_at,
  id AS fulfillment_id,
  name AS fulfillment_name,
  status,
  service,
  order_id,
  shop_url,
  created_at AS fulfillment_created_at,
  CAST(JSON_EXTRACT_SCALAR(line_item, '$.current_quantity') AS INT64) AS current_quantity,
  CAST(JSON_EXTRACT_SCALAR(line_item, '$.fulfillable_quantity') AS INT64) AS fulfillable_quantity,
  JSON_EXTRACT_SCALAR(line_item, '$.fulfillment_service') AS fulfillment_service,
  JSON_EXTRACT_SCALAR(line_item, '$.fulfillment_status') AS fulfillment_status,
  CAST(JSON_EXTRACT_SCALAR(line_item, '$.gift_card') AS BOOL) AS gift_card,
  JSON_EXTRACT_SCALAR(line_item, '$.name') AS product_name,
  CAST(JSON_EXTRACT_SCALAR(line_item, '$.price') AS FLOAT64) AS product_price,
  CAST(JSON_EXTRACT_SCALAR(line_item, '$.product_id') AS INT64) AS product_id,
  JSON_EXTRACT_SCALAR(line_item, '$.sku') AS product_sku,
  MAX(
    CASE
      WHEN JSON_EXTRACT_SCALAR(tax_line, '$.title') = "CGST" THEN CAST(JSON_EXTRACT_SCALAR(tax_line, '$.price') AS FLOAT64)
      ELSE NULL
    END
  ) AS cgst,
  MAX(
    CASE
      WHEN JSON_EXTRACT_SCALAR(tax_line, '$.title') = "SGST" THEN CAST(JSON_EXTRACT_SCALAR(tax_line, '$.price') AS FLOAT64)
      ELSE NULL
    END
  ) AS sgst,
  MAX(
    CASE
      WHEN JSON_EXTRACT_SCALAR(tax_line, '$.title') = "IGST" THEN CAST(JSON_EXTRACT_SCALAR(tax_line, '$.price') AS FLOAT64)
      ELSE NULL
    END
  ) AS igst,
  updated_at AS fulfillment_updated_at,
  location_id,
  tracking_url,
  notify_customer,
  shipment_status,
  tracking_number,
  tracking_company,
  admin_graphql_api_id,
  
  -- Composite key for uniqueness
  CONCAT(
    CAST(id AS STRING), '-', 
    CAST(order_id AS STRING), '-', 
    CAST(JSON_EXTRACT_SCALAR(line_item, '$.sku') AS STRING), '-', 
    CAST(JSON_EXTRACT_SCALAR(line_item, '$.product_id') AS STRING), '-', 
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', created_at)
  ) AS composite_key

FROM 
  `shopify-pubsub-project.pilgrim_bi_airbyte.fulfillments`,
  UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS line_item,
  UNNEST(JSON_EXTRACT_ARRAY(line_item, '$.tax_lines')) AS tax_line

WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)

GROUP BY
_airbyte_extracted_at,
fulfillment_id,
fulfillment_name,
status,
service,
order_id,
shop_url,
fulfillment_created_at,
current_quantity,
fulfillable_quantity,
fulfillment_service,
fulfillment_status,
gift_card,
product_name,
product_price,
product_id,
product_sku,
fulfillment_updated_at,
location_id,
tracking_url,
notify_customer,
shipment_status,
tracking_number,
tracking_company,
admin_graphql_api_id,
composite_key

 ) AS source
ON target.composite_key = source.composite_key

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.fulfillment_id = source.fulfillment_id,
target.fulfillment_name = source.fulfillment_name,
target.status = source.status,
target.service = source.service,
target.order_id = source.order_id,
target.shop_url = source.shop_url,
target.fulfillment_created_at = source.fulfillment_created_at,
target.current_quantity = source.current_quantity,
target.fulfillable_quantity = source.fulfillable_quantity,
target.fulfillment_service = source.fulfillment_service,
target.fulfillment_status = source.fulfillment_status,
target.gift_card = source.gift_card,
target.product_name = source.product_name,
target.product_price = source.product_price,
target.product_id = source.product_id,
target.product_sku = source.product_sku,
target.cgst = source.cgst,
target.sgst = source.sgst,
target.igst = source.igst,
target.fulfillment_updated_at = source.fulfillment_updated_at,
target.location_id = source.location_id,
target.tracking_url = source.tracking_url,
target.notify_customer = source.notify_customer,
target.shipment_status = source.shipment_status,
target.tracking_number = source.tracking_number,
target.tracking_company = source.tracking_company,
target.admin_graphql_api_id = source.admin_graphql_api_id,
target.composite_key = source.composite_key

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
fulfillment_id,
fulfillment_name,
status,
service,
order_id,
shop_url,
fulfillment_created_at,
current_quantity,
fulfillable_quantity,
fulfillment_service,
fulfillment_status,
gift_card,
product_name,
product_price,
product_id,
product_sku,
cgst,
sgst,
igst,
fulfillment_updated_at,
location_id,
tracking_url,
notify_customer,
shipment_status,
tracking_number,
tracking_company,
admin_graphql_api_id,
composite_key

 )

  VALUES (
source._airbyte_extracted_at,
source.fulfillment_id,
source.fulfillment_name,
source.status,
source.service,
source.order_id,
source.shop_url,
source.fulfillment_created_at,
source.current_quantity,
source.fulfillable_quantity,
source.fulfillment_service,
source.fulfillment_status,
source.gift_card,
source.product_name,
source.product_price,
source.product_id,
source.product_sku,
source.cgst,
source.sgst,
source.igst,
source.fulfillment_updated_at,
source.location_id,
source.tracking_url,
source.notify_customer,
source.shipment_status,
source.tracking_number,
source.tracking_company,
source.admin_graphql_api_id,
source.composite_key

  )
