
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_CANCELLED_LISTINGS_DATA` AS target  
USING (
SELECT  
distinct
_airbyte_extracted_at,
price,
quantity,
item_name,
product_id,
seller_sku,
dataEndTime,
Business_Price,
item_condition,
product_id_type,
item_description,
item_is_marketplace
FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_MERCHANT_CANCELLED_LISTINGS_DATA`

WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.product_id = source.product_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET
  
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.price = source.price,
target.quantity = source.quantity,
target.item_name = source.item_name,
target.product_id = source.product_id,
target.seller_sku = source.seller_sku,
target.dataEndTime = source.dataEndTime,
target.Business_Price = source.Business_Price,
target.item_condition = source.item_condition,
target.product_id_type = source.product_id_type,
target.item_description = source.item_description,
target.item_is_marketplace = source.item_is_marketplace

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
price,
quantity,
item_name,
product_id,
seller_sku,
dataEndTime,
Business_Price,
item_condition,
product_id_type,
item_description,
item_is_marketplace
    
  )
  VALUES (
  source._airbyte_extracted_at,
  source.price,
  source.quantity,
  source.item_name,
  source.product_id,
  source.seller_sku,
  source.dataEndTime,
  source.Business_Price,
  source.item_condition,
  source.product_id_type,
  source.item_description,
  source.item_is_marketplace
  );
