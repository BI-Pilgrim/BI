MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.portfolios` AS target
USING (
  SELECT 
    _airbyte_extracted_at,
    name,
    state,
    inBudget,
    portfolioId,
    DATE(TIMESTAMP_MICROS(CAST(creationDate AS INT64) * 1000)) AS creationDate,
    servingStatus,
    DATE(TIMESTAMP_MICROS(CAST(lastUpdatedDate AS INT64) * 1000)) AS lastUpdatedDate
  FROM (
    SELECT *, 
      ROW_NUMBER() OVER (PARTITION BY portfolioId ORDER BY _airbyte_extracted_at DESC) AS rn
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.portfolios`
  )
  WHERE rn = 1
  AND _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at) 
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.portfolios`
  )
) AS source
ON FALSE  
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    name,
    state,
    inBudget,
    portfolioId,
    creationDate,
    servingStatus,
    lastUpdatedDate
  )
  VALUES (
    source._airbyte_extracted_at,
    source.name,
    source.state,
    source.inBudget,
    source.portfolioId,
    source.creationDate,
    source.servingStatus,
    source.lastUpdatedDate
  );
