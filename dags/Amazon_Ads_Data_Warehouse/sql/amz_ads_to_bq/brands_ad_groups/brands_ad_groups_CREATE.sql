CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_ad_groups` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT  
_airbyte_extracted_at,
name,
state,
adGroupId,
campaignId,
--extendedData 
FROM (
  SELECT *, 
  ROW_NUMBER() OVER(PARTITION BY adGroupId) as rn
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_brands_ad_groups`
)