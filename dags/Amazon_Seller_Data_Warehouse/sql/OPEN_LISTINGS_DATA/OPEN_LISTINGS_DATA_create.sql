
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.OPEN_LISTINGS_DATA`
PARTITION BY dataEndTime
CLUSTER BY ASIN_
OPTIONS(
  description = "OPEN LISTINGS DATA table is partitioned on Data End Time",
  require_partition_filter = FALSE
)
 AS 
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
