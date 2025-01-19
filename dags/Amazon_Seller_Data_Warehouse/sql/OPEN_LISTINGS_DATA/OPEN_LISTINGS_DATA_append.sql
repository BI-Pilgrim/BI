
MERGE INTO  `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.OPEN_LISTINGS_DATA` AS target

USING (
SELECT  
distinct
_airbyte_extracted_at,
sku,
asin as ASIN_,
price,
quantity,
dataEndTime,
Business_Price,
Quantity_Price_1 as Quantity_price,
Quantity_Price_Type,
Quantity_Lower_Bound_1 as Lower_bound_quantity

FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_OPEN_LISTINGS_DATA`

WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.ASIN_ = source.ASIN_

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.sku = source.sku,
target.ASIN_ = source.ASIN_,
target.price = source.price,
target.quantity = source.quantity,
target.dataEndTime = source.dataEndTime,
target.Business_Price = source.Business_Price,
target.Quantity_price = source.Quantity_price,
target.Quantity_Price_Type = source.Quantity_Price_Type,
target.Lower_bound_quantity = source.Lower_bound_quantity


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
sku,
ASIN_,
price,
quantity,
dataEndTime,
Business_Price,
Quantity_price,
Quantity_Price_Type,
Lower_bound_quantity

 )

  VALUES (
source._airbyte_extracted_at,
source.sku,
source.ASIN_,
source.price,
source.quantity,
source.dataEndTime,
source.Business_Price,
source.Quantity_price,
source.Quantity_Price_Type,
source.Lower_bound_quantity
  )
