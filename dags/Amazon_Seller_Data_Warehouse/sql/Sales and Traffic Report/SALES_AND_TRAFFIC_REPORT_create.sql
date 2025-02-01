
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.SALES_AND_TRAFFIC_REPORT`
PARTITION BY DATE_TRUNC(queryEndDate, DAY)
CLUSTER BY parentAsin
OPTIONS(
  description = "Sales and traffic table is partitioned on query end date",
  require_partition_filter = FALSE
)
AS 
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

FROM 
  `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_SALES_AND_TRAFFIC_REPORT`;
