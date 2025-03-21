MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_ad_groups` AS target
USING (
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
  FROM 
    `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_display_ad_groups`
  WHERE 
    _airbyte_extracted_at > (
      SELECT MAX(_airbyte_extracted_at) 
      FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_ad_groups`
    )
) AS source
ON FALSE 
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    name,
    state,
    tactic,
    adGroupId,
    campaignId,
    defaultBid,
    creativeType,
    bidOptimization
  )
  VALUES (
    source._airbyte_extracted_at,
    source.name,
    source.state,
    source.tactic,
    source.adGroupId,
    source.campaignId,
    source.defaultBid,
    source.creativeType,
    source.bidOptimization
  );
