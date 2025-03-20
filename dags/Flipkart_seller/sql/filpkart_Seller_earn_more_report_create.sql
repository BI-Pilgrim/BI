CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_flipkart_seller_staging.earn_more_report` AS
SELECT DISTINCT * EXCEPT(runid) 
FROM `shopify-pubsub-project.pilgrim_bi_flipkart_seller.earn_more_report`;
