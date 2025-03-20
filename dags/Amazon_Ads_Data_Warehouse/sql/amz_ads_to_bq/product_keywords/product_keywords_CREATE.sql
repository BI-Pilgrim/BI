CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_keywords` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT 
_airbyte_extracted_at,
state,
adGroupId,
keywordId,
campaignId,
keywordText,
nativeLanguageLocale 
FROM (
  SELECT *,
  ROW_NUMBER() OVER(PARTITION BY keywordId ) AS rn
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_keywords`
) where rn = 1