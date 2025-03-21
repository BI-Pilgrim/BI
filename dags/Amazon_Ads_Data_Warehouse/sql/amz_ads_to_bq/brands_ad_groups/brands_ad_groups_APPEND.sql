MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_ad_groups` AS target
USING (
  SELECT 
    _airbyte_extracted_at,
    name,
    state,
    adGroupId,
    campaignId,
    --extendedData
  FROM (
    SELECT 
      _airbyte_extracted_at,
      name,
      state,
      adGroupId,
      campaignId,
      --extendedData,
      ROW_NUMBER() OVER(PARTITION BY adGroupId) AS rn
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_brands_ad_groups`
  )
  WHERE --rn = 1AND
   _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at) 
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_ad_groups`
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
    --source.extendedData
  );
