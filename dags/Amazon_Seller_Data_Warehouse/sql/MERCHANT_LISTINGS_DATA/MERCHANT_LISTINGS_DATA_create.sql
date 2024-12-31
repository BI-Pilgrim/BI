
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_LISTINGS_DATA`
PARTITION BY DATE_TRUNC(open_date, DAY)
CLUSTER BY listing_id
OPTIONS(
  description = "MERCHANT_LISTINGS_DATA table is partitioned on open_date ",
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
product_id_type,
Quantity_Price_1 as Quantity_price,
item_condition,
item_description,
Quantity_Lower_Bound_1 as Lower_bound_quantity,
fulfillment_channel,
item_is_marketplace
from `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_MERCHANT_LISTINGS_DATA`

