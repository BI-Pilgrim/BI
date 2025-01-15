
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.SALES_AND_TRAFFIC_REPORT`
PARTITION BY DATE_TRUNC(queryEndDate,DAY)
CLUSTER BY parentAsin
OPTIONS(
  description = "sales and traffic table is partitioned on query end Date ",
  require_partition_filter = FALSE
)
 AS 
SELECT  
distinct
_airbyte_extracted_at,
sku,
childAsin,
parentAsin,
queryEndDate,
json_extract_scalar(trafficByAsin,'$.browserPageViews') as browser_page_view,
json_extract_scalar(trafficByAsin,'$.browserSessions') as browser_sessions,
json_extract_scalar(trafficByAsin,'$.mobileAppPageViews') as app_page_view,
json_extract_scalar(trafficByAsin,'$.pageViews') as page_view,
json_extract_scalar(trafficByAsin,'$.sessions') as sessions,
CAST(JSON_EXTRACT_SCALAR(salesByAsin, '$.orderedProductSales.amount') AS FLOAT64) AS ordered_product_sales,
CAST(JSON_EXTRACT_SCALAR(salesByAsin, '$.orderedProductSalesB2B.amount') AS FLOAT64) AS ordered_product_sales_b2b,
CAST(JSON_EXTRACT_SCALAR(salesByAsin, '$.totalOrderItems') AS INT64) AS total_order_items,
CAST(JSON_EXTRACT_SCALAR(salesByAsin, '$.totalOrderItemsB2B') AS INT64) AS total_order_items_b2b


 FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_SALES_AND_TRAFFIC_REPORT` 
