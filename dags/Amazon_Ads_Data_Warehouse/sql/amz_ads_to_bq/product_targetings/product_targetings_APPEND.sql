MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_targetings` AS target
USING (
  SELECT 
    _airbyte_extracted_at,
    bid,
    state,
    targetId,
    adGroupId,
    campaignId,
    JSON_EXTRACT_SCALAR(item, '$.type') AS extracted_type,
    JSON_EXTRACT_SCALAR(item, '$.value') AS extracted_value,
    expressionType
  FROM (
    SELECT *, 
      ROW_NUMBER() OVER(PARTITION BY targetId ORDER BY _airbyte_extracted_at DESC) AS row_num
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_targetings`,
    UNNEST(JSON_EXTRACT_ARRAY(resolvedExpression)) AS item
  )
  WHERE row_num = 1
  AND _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at)
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_targetings`
  )
) AS source
ON FALSE  
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    bid,
    state,
    targetId,
    adGroupId,
    campaignId,
    extracted_type,
    extracted_value,
    expressionType
  )
  VALUES (
    source._airbyte_extracted_at,
    source.bid,
    source.state,
    source.targetId,
    source.adGroupId,
    source.campaignId,
    source.extracted_type,
    source.extracted_value,
    source.expressionType
  );
