

MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` AS target

USING (
  SELECT  
distinct
_airbyte_extracted_at,
sku,
asin,
currency,
item_tax,
quantity,
ship_city,
item_price,
ship_state,
dataEndTime,
item_status,
order_status,
product_name,
ship_country,
shipping_tax,
order_channel,
promotion_ids,
purchase_date,
sales_channel,
shipping_price,
amazon_order_id as Amazon_Order_id,
gift_wrap_price,
ship_postal_code,
is_business_order,
last_updated_date,
merchant_order_id,
price_designation,
ship_service_level,
fulfillment_channel,
purchase_order_number,
item_promotion_discount,
ship_promotion_discount

FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`
    
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.Amazon_Order_id = source.Amazon_Order_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.sku = source.sku,
target.asin = source.asin,
target.currency = source.currency,
target.item_tax = source.item_tax,
target.quantity = source.quantity,
target.ship_city = source.ship_city,
target.item_price = source.item_price,
target.ship_state = source.ship_state,
target.dataEndTime = source.dataEndTime,
target.item_status = source.item_status,
target.order_status = source.order_status,
target.product_name = source.product_name,
target.ship_country = source.ship_country,
target.shipping_tax = source.shipping_tax,
target.order_channel = source.order_channel,
target.promotion_ids = source.promotion_ids,
target.purchase_date = source.purchase_date,
target.sales_channel = source.sales_channel,
target.shipping_price = source.shipping_price,
target.Amazon_Order_id = source.Amazon_Order_id,
target.gift_wrap_price = source.gift_wrap_price,
target.ship_postal_code = source.ship_postal_code,
target.is_business_order = source.is_business_order,
target.last_updated_date = source.last_updated_date,
target.merchant_order_id = source.merchant_order_id,
target.price_designation = source.price_designation,
target.ship_service_level = source.ship_service_level,
target.fulfillment_channel = source.fulfillment_channel,
target.purchase_order_number = source.purchase_order_number,
target.item_promotion_discount = source.item_promotion_discount,
target.ship_promotion_discount = source.ship_promotion_discount


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
sku,
asin,
currency,
item_tax,
quantity,
ship_city,
item_price,
ship_state,
dataEndTime,
item_status,
order_status,
product_name,
ship_country,
shipping_tax,
order_channel,
promotion_ids,
purchase_date,
sales_channel,
shipping_price,
Amazon_Order_id,
gift_wrap_price,
ship_postal_code,
is_business_order,
last_updated_date,
merchant_order_id,
price_designation,
ship_service_level,
fulfillment_channel,
purchase_order_number,
item_promotion_discount,
ship_promotion_discount
   )

  VALUES (
source._airbyte_extracted_at,
source.sku,
source.asin,
source.currency,
source.item_tax,
source.quantity,
source.ship_city,
source.item_price,
source.ship_state,
source.dataEndTime,
source.item_status,
source.order_status,
source.product_name,
source.ship_country,
source.shipping_tax,
source.order_channel,
source.promotion_ids,
source.purchase_date,
source.sales_channel,
source.shipping_price,
source.Amazon_Order_id,
source.gift_wrap_price,
source.ship_postal_code,
source.is_business_order,
source.last_updated_date,
source.merchant_order_id,
source.price_designation,
source.ship_service_level,
source.fulfillment_channel,
source.purchase_order_number,
source.item_promotion_discount,
source.ship_promotion_discount

  )
