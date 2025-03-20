MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_keywords` AS target
USING (
  SELECT 
    _airbyte_extracted_at,
    name,
    state,
    adGroupId,
    campaignId
  FROM (
    SELECT 
      _airbyte_extracted_at,
      name,
      state,
      adGroupId,
      campaignId,
      ROW_NUMBER() OVER (PARTITION BY campaignId) AS rn
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_brands_keywords`
  )
  WHERE rn = 1
  AND _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at) 
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_keywords`
  )
) AS source
ON FALSE 
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at, 
    name,
    state,
    adGroupId,
    campaignId
  )
  VALUES (
    source._airbyte_extracted_at, 
    source.name, 
    source.state, 
    source.adGroupId, 
    source.campaignId
  );
