
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`
PARTITION BY DATE_TRUNC(purchase_date,DAY)
CLUSTER BY Amazon_Order_id
OPTIONS(
  description = "ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL table is partitioned on Purchase Date ",
  require_partition_filter = FALSE
)
 AS 
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
FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`
