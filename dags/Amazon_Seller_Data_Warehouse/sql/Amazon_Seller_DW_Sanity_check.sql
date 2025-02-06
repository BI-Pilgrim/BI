
select * from `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.Amazon_seller_Sanity_check`


CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.Amazon_seller_Sanity_check` AS
WITH Sources AS (
  SELECT 
    'ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL' AS Source_table_name,
    TIMESTAMP(MAX(purchase_date)) AS Source_max_date,  -- Cast to TIMESTAMP
    COUNT(DISTINCT CASE 
        WHEN DATE(purchase_date) = (SELECT MAX(DATE(purchase_date)) 
                                    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`) 
        THEN amazon_order_id 
    END) AS Source_pk_count
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`

  UNION ALL

  SELECT 
    'ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL' AS Source_table_name,
    TIMESTAMP(MAX(purchase_date)) AS Source_max_date,  -- Cast to TIMESTAMP
    COUNT(DISTINCT CASE 
        WHEN DATE(purchase_date) = (SELECT MAX(DATE(purchase_date)) 
                                    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`) 
        THEN amazon_order_id 
    END) AS Source_pk_count
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`

  UNION ALL

  SELECT 
    'ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML' AS Source_table_name,
    TIMESTAMP(MAX(PurchaseDate)) AS Source_max_date,  -- Cast to TIMESTAMP
    COUNT(DISTINCT CASE 
        WHEN DATE(PurchaseDate) = (SELECT MAX(DATE(PurchaseDate)) 
                                   FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML`) 
        THEN AmazonOrderID 
    END) AS Source_pk_count
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`

  UNION ALL

  SELECT 
    'MERCHANT_CANCELLED_LISTINGS_DATA' AS Source_table_name,
    TIMESTAMP(MAX(dataEndTime)) AS Source_max_date,  -- Cast to TIMESTAMP
    COUNT(DISTINCT CASE 
        WHEN DATE(dataEndTime) = (SELECT MAX(DATE(dataEndTime)) 
                                  FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_CANCELLED_LISTINGS_DATA`) 
        THEN product_id 
    END) AS Source_pk_count
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.GET_MERCHANT_CANCELLED_LISTINGS_DATA`
),

Destination AS (
  SELECT 
    'ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL' AS Dest_table_name,
    TIMESTAMP(MAX(purchase_date)) AS Dest_max_date,  -- Cast to TIMESTAMP
    COUNT(DISTINCT CASE 
        WHEN DATE(purchase_date) = (SELECT MAX(DATE(purchase_date)) 
                                    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`) 
        THEN amazon_order_id 
    END) AS Dest_pk_count
  FROM  `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`

  UNION ALL

  SELECT 
    'ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL' AS Dest_table_name,
    TIMESTAMP(MAX(purchase_date)) AS Dest_max_date,  -- Cast to TIMESTAMP
    COUNT(DISTINCT CASE 
        WHEN DATE(purchase_date) = (SELECT MAX(DATE(purchase_date)) 
                                    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`) 
        THEN amazon_order_id 
    END) AS Dest_pk_count
  FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL`

  UNION ALL

  SELECT 
    'ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML' AS Dest_table_name,
    TIMESTAMP(MAX(PurchaseDate)) AS Dest_max_date,  -- Cast to TIMESTAMP
    COUNT(DISTINCT CASE 
        WHEN DATE(PurchaseDate) = (SELECT MAX(DATE(PurchaseDate)) 
                                   FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML`) 
        THEN amazon_order_id 
    END) AS Dest_pk_count
  FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL_XML`

  UNION ALL

  SELECT 
    'MERCHANT_CANCELLED_LISTINGS_DATA' AS Dest_table_name,
    TIMESTAMP(MAX(dataEndTime)) AS Dest_max_date,  -- Cast to TIMESTAMP
    COUNT(DISTINCT CASE 
        WHEN DATE(dataEndTime) = (SELECT MAX(DATE(dataEndTime)) 
                                  FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_CANCELLED_LISTINGS_DATA`) 
        THEN product_id 
    END) AS Dest_pk_count
  FROM `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.MERCHANT_CANCELLED_LISTINGS_DATA`
)

SELECT 
  s.Source_table_name,
  s.Source_max_date,
  s.Source_pk_count,
  d.Dest_table_name,
  d.Dest_max_date,
  d.Dest_pk_count
FROM Sources s
LEFT JOIN Destination d 
ON s.Source_table_name = d.Dest_table_name;
