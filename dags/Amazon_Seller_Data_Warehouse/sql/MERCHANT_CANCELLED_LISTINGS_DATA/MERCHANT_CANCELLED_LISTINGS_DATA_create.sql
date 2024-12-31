
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_CANCELLED_LISTINGS_DATA`
PARTITION BY dataEndTime
CLUSTER BY product_id
OPTIONS(
  description = "GET_MERCHANT_CANCELLED_LISTINGS_DATA table is partitioned on dataEndTime",
  require_partition_filter = FALSE
)
 AS 
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
