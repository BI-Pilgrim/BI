CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_ads` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT 
_airbyte_extracted_at,
sku,
adId,
asin,
state,
adGroupId,
campaignId,
customText,
FROM 
(
  SELECT *, 
  ROW_NUMBER() OVER(PARTITION BY adId) as rn
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_ads`
) 
WHERE rn =1 