MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_ad_groups` AS target
USING (
  SELECT 
    _airbyte_extracted_at,
    name,
    state,
    adGroupId,
    campaignId,
    defaultBid
  FROM (
    SELECT *, 
      ROW_NUMBER() OVER (PARTITION BY adGroupId, campaignId ORDER BY _airbyte_extracted_at DESC) AS rn
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_ad_groups`
  )
  WHERE rn = 1
  AND _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at) 
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_ad_groups`
  )
) AS source
ON FALSE  
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    name,
    state,
    adGroupId,
    campaignId,
    defaultBid
  )
  VALUES (
    source._airbyte_extracted_at,
    source.name,
    source.state,
    source.adGroupId,
    source.campaignId,
    source.defaultBid
  );
