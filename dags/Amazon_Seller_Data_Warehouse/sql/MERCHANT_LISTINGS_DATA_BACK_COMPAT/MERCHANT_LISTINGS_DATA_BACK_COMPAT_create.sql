
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_LISTINGS_DATA_BACK_COMPAT`
PARTITION BY DATE_TRUNC(open_date, DAY)
CLUSTER BY listing_id
OPTIONS(
  description = "MERCHANT_LISTINGS_DATA_BACK_COMPAT table is partitioned on Open date ",
  require_partition_filter = FALSE
)
 AS 
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
item_condition,
product_id_type,
item_description,
pending_quantity,
item_is_marketplace
FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT`