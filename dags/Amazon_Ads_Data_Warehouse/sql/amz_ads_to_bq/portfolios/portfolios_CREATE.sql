CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.portfolios` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT 
_airbyte_extracted_at,
name,
state,
inBudget,
portfolioId,
DATE(TIMESTAMP_MICROS(CAST(creationDate AS INT64) * 1000)) AS creationDate,
servingStatus,
DATE(TIMESTAMP_MICROS(CAST(lastUpdatedDate AS INT64) * 1000)) AS lastUpdatedDate,
FROM(
  SELECT *, 
  ROW_NUMBER() OVER (PARTITION BY portfolioId ) AS rn
   FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.portfolios`
) where rn=1 