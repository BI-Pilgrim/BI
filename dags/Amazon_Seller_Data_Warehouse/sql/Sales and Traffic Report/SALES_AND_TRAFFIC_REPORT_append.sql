

MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.SALES_AND_TRAFFIC_REPORT` AS target

USING (
SELECT  
  DISTINCT
  _airbyte_extracted_at,
  sku,
  childAsin,
  parentAsin,
  queryEndDate,
  JSON_EXTRACT_SCALAR(trafficByAsin, '$.browserPageViews') AS browser_page_view,
  JSON_EXTRACT_SCALAR(trafficByAsin, '$.browserSessions') AS browser_sessions,
  JSON_EXTRACT_SCALAR(trafficByAsin, '$.mobileAppPageViews') AS app_page_view,
  JSON_EXTRACT_SCALAR(trafficByAsin, '$.pageViews') AS page_view,
  JSON_EXTRACT_SCALAR(trafficByAsin, '$.sessions') AS sessions,
  CAST(JSON_EXTRACT_SCALAR(salesByAsin, '$.orderedProductSales.amount') AS FLOAT64) AS ordered_product_sales,
  CAST(JSON_EXTRACT_SCALAR(salesByAsin, '$.orderedProductSalesB2B.amount') AS FLOAT64) AS ordered_product_sales_b2b,
  CAST(JSON_EXTRACT_SCALAR(salesByAsin, '$.totalOrderItems') AS INT64) AS total_order_items,
  CAST(JSON_EXTRACT_SCALAR(salesByAsin, '$.totalOrderItemsB2B') AS INT64) AS total_order_items_b2b,

  -- Composite key for uniqueness
  CONCAT(
    COALESCE(parentAsin, 'NULL'), '-', 
    FORMAT_TIMESTAMP('%Y-%m-%d', queryEndDate)
  ) AS composite_key


 FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_SALES_AND_TRAFFIC_REPORT` 

  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.composite_key = source.composite_key


WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.sku = source.sku,
target.childAsin = source.childAsin,
target.parentAsin = source.parentAsin,
target.queryEndDate = source.queryEndDate,
target.browser_page_view = source.browser_page_view,
target.browser_sessions = source.browser_sessions,
target.app_page_view = source.app_page_view,
target.page_view = source.page_view,
target.sessions = source.sessions,
target.ordered_product_sales = source.ordered_product_sales,
target.ordered_product_sales_b2b = source.ordered_product_sales_b2b,
target.total_order_items = source.total_order_items,
target.total_order_items_b2b = source.total_order_items_b2b,
target.composite_key = source.composite_key

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
sku,
childAsin,
parentAsin,
queryEndDate,
browser_page_view,
browser_sessions,
app_page_view,
page_view,
sessions,
ordered_product_sales,
ordered_product_sales_b2b,
total_order_items,
total_order_items_b2b,
composite_key

  )

  VALUES (
source._airbyte_extracted_at,
source.sku,
source.childAsin,
source.parentAsin,
source.queryEndDate,
source.browser_page_view,
source.browser_sessions,
source.app_page_view,
source.page_view,
source.sessions,
source.ordered_product_sales,
source.ordered_product_sales_b2b,
source.total_order_items,
source.total_order_items_b2b,
source.composite_key
  )
