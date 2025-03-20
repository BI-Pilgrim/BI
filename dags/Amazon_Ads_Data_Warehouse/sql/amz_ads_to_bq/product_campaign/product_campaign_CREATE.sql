CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_campaign` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT 
_airbyte_extracted_at,
name,
state,
JSON_EXTRACT_SCALAR(budget, '$.budget') AS budget,
JSON_EXTRACT_SCALAR(budget, '$.budgetType') AS budgetType,
endDate,
startDate,
campaignId,
portfolioId,
targetingType, 
placement_bidding.percentage AS placement_percentage,
placement_bidding.placement AS placement,
JSON_EXTRACT_SCALAR(dynamicBidding, '$.strategy') AS strategy,
FROM 
(
SELECT
  * 
FROM 
  `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_campaigns` spc
LEFT JOIN
  UNNEST(JSON_EXTRACT_ARRAY(spc.dynamicBidding, '$.placementBidding')) AS placement_bidding

)

