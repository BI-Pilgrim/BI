CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_campaigns` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT  
_airbyte_extracted_at,
name,
tags,
state,
budget,
JSON_EXTRACT_SCALAR(bidding, '$.bidOptimization') AS bidOptimization,
JSON_EXTRACT_SCALAR(bidding, '$.bidOptimizationStrategy') AS bidOptimizationStrategy,
endDate,
costType,
startDate,
budgetType,
campaignId,
portfolioId,
extendedData,
JSON_EXTRACT_SCALAR(smartDefault, '$[0]') AS smartDefault,
brandEntityId,
productLocation,
ruleBasedBudget 
FROM (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY campaignId) AS rn
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_brands_campaigns`
) WHERE rn = 1