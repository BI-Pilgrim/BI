
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_LISTINGS_DATA` AS target  
USING (
SELECT  
distinct
_airbyte_extracted_at,
asin1,
price,
quantity,
item_name,
open_date,
listing_id,
product_id,
seller_sku,
dataEndTime,
Business_Price,
product_id_type,
Quantity_Price_1 as Quantity_price,
item_condition,
item_description,
Quantity_Lower_Bound_1 as Lower_bound_quantity,
fulfillment_channel,
item_is_marketplace
from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_MERCHANT_LISTINGS_DATA`


WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.listing_id = source.listing_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.asin1 = source.asin1,
target.price = source.price,
target.quantity = source.quantity,
target.item_name = source.item_name,
target.open_date = source.open_date,
target.listing_id = source.listing_id,
target.product_id = source.product_id,
target.seller_sku = source.seller_sku,
target.dataEndTime = source.dataEndTime,
target.Business_Price = source.Business_Price,
target.product_id_type = source.product_id_type,
target.Quantity_price = source.Quantity_price,
target.item_condition = source.item_condition,
target.item_description = source.item_description,
target.Lower_bound_quantity = source.Lower_bound_quantity,
target.fulfillment_channel = source.fulfillment_channel,
target.item_is_marketplace = source.item_is_marketplace

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
asin1,
price,
quantity,
item_name,
open_date,
listing_id,
product_id,
seller_sku,
dataEndTime,
Business_Price,
product_id_type,
Quantity_price,
item_condition,
item_description,
Lower_bound_quantity,
fulfillment_channel,
item_is_marketplace
    
  )
  VALUES (
  source._airbyte_extracted_at,
  source.asin1,
  source.price,
  source.quantity,
  source.item_name,
  source.open_date,
  source.listing_id,
  source.product_id,
  source.seller_sku,
  source.dataEndTime,
  source.Business_Price,
  source.product_id_type,
  source.Quantity_price,
  source.item_condition,
  source.item_description,
  source.Lower_bound_quantity,
  source.fulfillment_channel,
  source.item_is_marketplace
 
  );
