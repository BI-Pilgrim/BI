
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_LISTINGS_ALL_DATA` AS target  
USING (
SELECT  
distinct
_airbyte_extracted_at,
asin1,
price,
status,
quantity,
item_name,
open_date,
listing_id,
product_id,
seller_sku,
dataEndTime,
item_condition,
product_id_type,
item_description,
pending_quantity,
fulfillment_channel,
item_is_marketplace
from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_MERCHANT_LISTINGS_ALL_DATA`


WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.listing_id = source.listing_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.asin1 = source.asin1,
target.price = source.price,
target.status = source.status,
target.quantity = source.quantity,
target.item_name = source.item_name,
target.open_date = source.open_date,
target.listing_id = source.listing_id,
target.product_id = source.product_id,
target.seller_sku = source.seller_sku,
target.dataEndTime = source.dataEndTime,
target.item_condition = source.item_condition,
target.product_id_type = source.product_id_type,
target.item_description = source.item_description,
target.pending_quantity = source.pending_quantity,
target.fulfillment_channel = source.fulfillment_channel,
target.item_is_marketplace = source.item_is_marketplace

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
asin1,
price,
status,
quantity,
item_name,
open_date,
listing_id,
product_id,
seller_sku,
dataEndTime,
item_condition,
product_id_type,
item_description,
pending_quantity,
fulfillment_channel,
item_is_marketplace
   
  )
  VALUES (
  source._airbyte_extracted_at,
  source.asin1,
  source.price,
  source.status,
  source.quantity,
  source.item_name,
  source.open_date,
  source.listing_id,
  source.product_id,
  source.seller_sku,
  source.dataEndTime,
  source.item_condition,
  source.product_id_type,
  source.item_description,
  source.pending_quantity,
  source.fulfillment_channel,
  source.item_is_marketplace
 
  );
