CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_campaigns` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT
_airbyte_extracted_at,
name,
state,
budget,
tactic,
endDate,
costType,
PARSE_DATE('%Y%m%d', startdate) AS Start_Date,
budgetType,
campaignId,
portfolioId,
deliveryProfile 
FROM 
(
  SELECT * , 
  ROW_NUMBER() OVER(PARTITION BY campaignId) as rn
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_display_campaigns`
) where rn = 1