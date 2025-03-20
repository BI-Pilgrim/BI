CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_ad_groups` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT
_airbyte_extracted_at,
name,
state,
tactic,
adGroupId,
campaignId,
defaultBid,
creativeType,
bidOptimization 
FROM (
SELECT * 
FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_display_ad_groups`
)