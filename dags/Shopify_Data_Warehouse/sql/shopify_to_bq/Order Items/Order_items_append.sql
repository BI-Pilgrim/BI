MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items` AS target

USING (
  SELECT DISTINCT
    _airbyte_extracted_at,
    CAST(id AS STRING) AS Order_id,
    CAST(order_number AS STRING) AS Order_number,
    name AS Order_name,
    updated_at AS Order_updated_at,
    created_at AS Order_created_at,
    CAST(processed_at AS TIMESTAMP) AS Order_processed_at,
    JSON_EXTRACT_SCALAR(customer, '$.id') AS customer_id,
    CAST(JSON_EXTRACT_SCALAR(tax_lines, '$[0].price') AS FLOAT64) AS order_tax_price,
    JSON_EXTRACT_SCALAR(item, '$.name') AS item_name,
    CAST(JSON_EXTRACT_SCALAR(item, '$.price') AS FLOAT64) AS item_price,
    CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS INT64) AS item_quantity,
    JSON_EXTRACT_SCALAR(item, '$.sku') AS item_sku_code,
    CAST(JSON_EXTRACT_SCALAR(item, '$.total_discount') AS FLOAT64) AS item_discount,
    CAST(JSON_EXTRACT_SCALAR(item, '$.variant_id') AS INT64) AS item_variant_id,
    JSON_EXTRACT_SCALAR(item, '$.fulfillment_status') AS item_fulfillment_status
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte.orders`,
    UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS item
  WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.Order_id = source.Order_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  target._airbyte_extracted_at = source._airbyte_extracted_at,
  target.Order_id = source.Order_id,
  target.Order_number = source.Order_number,
  target.Order_name = source.Order_name,
  target.Order_updated_at = source.Order_updated_at,
  target.Order_created_at = source.Order_created_at,
  target.Order_processed_at = source.Order_processed_at,
  target.customer_id = source.customer_id,
  target.order_tax_price = source.order_tax_price,
  target.item_name = source.item_name,
  target.item_price = source.item_price,
  target.item_quantity = source.item_quantity,
  target.item_sku_code = source.item_sku_code,
  target.item_discount = source.item_discount,
  target.item_variant_id = source.item_variant_id,
  target.item_fulfillment_status = source.item_fulfillment_status

WHEN NOT MATCHED THEN INSERT (
  _airbyte_extracted_at,
  Order_id,
  Order_number,
  Order_name,
  Order_updated_at,
  Order_created_at,
  Order_processed_at,
  customer_id,
  order_tax_price,
  item_name,
  item_price,
  item_quantity,
  item_sku_code,
  item_discount,
  item_variant_id,
  item_fulfillment_status
)
VALUES (
  source._airbyte_extracted_at,
  source.Order_id,
  source.Order_number,
  source.Order_name,
  source.Order_updated_at,
  source.Order_created_at,
  source.Order_processed_at,
  source.customer_id,
  source.order_tax_price,
  source.item_name,
  source.item_price,
  source.item_quantity,
  source.item_sku_code,
  source.item_discount,
  source.item_variant_id,
  source.item_fulfillment_status
);
