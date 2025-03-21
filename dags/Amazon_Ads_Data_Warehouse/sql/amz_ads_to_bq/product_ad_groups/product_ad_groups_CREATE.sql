CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_ad_groups` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT 
_airbyte_extracted_at,
name,
state,
adGroupId,
campaignId,
defaultBid,
FROM 
(
  SELECT *, 
  ROW_NUMBER() OVER (PARTITION BY adGroupId,campaignId  ) AS rn
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_ad_groups`
) 
where rn = 1