CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_product_ads` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT 
_airbyte_extracted_at,
sku,
adId,
asin,
state,
adGroupId,
campaignId 
FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY adId) AS rn
FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_display_product_ads`
) WHERE rn = 1