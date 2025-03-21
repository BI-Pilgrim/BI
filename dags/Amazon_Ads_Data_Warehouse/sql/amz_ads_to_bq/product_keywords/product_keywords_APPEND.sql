MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_keywords` AS target
USING (
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
      ROW_NUMBER() OVER(PARTITION BY keywordId ORDER BY _airbyte_extracted_at DESC) AS rn
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_keywords`
  ) 
  WHERE rn = 1
  AND _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at) 
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_keywords`
  )
) AS source
ON FALSE  
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    state,
    adGroupId,
    keywordId,
    campaignId,
    keywordText,
    nativeLanguageLocale
  )
  VALUES (
    source._airbyte_extracted_at,
    source.state,
    source.adGroupId,
    source.keywordId,
    source.campaignId,
    source.keywordText,
    source.nativeLanguageLocale
  );
