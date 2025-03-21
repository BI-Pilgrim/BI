MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_campaigns` AS target
USING (
  SELECT 
    _airbyte_extracted_at,
    name,
    state,
    budget,
    JSON_EXTRACT_SCALAR(bidding, '$.bidOptimization') AS bidOptimization,
    JSON_EXTRACT_SCALAR(bidding, '$.bidOptimizationStrategy') AS bidOptimizationStrategy,
    endDate,
    costType,
    startDate,
    budgetType,
    campaignId,
    portfolioId,
    JSON_EXTRACT_SCALAR(smartDefault, '$[0]') AS smartDefault,
    brandEntityId,
    productLocation
  FROM (
    SELECT 
      _airbyte_extracted_at,
      name,
      state,
      budget,
      bidding,
      endDate,
      costType,
      startDate,
      budgetType,
      campaignId,
      portfolioId,
      smartDefault,
      brandEntityId,
      productLocation,
      ROW_NUMBER() OVER (PARTITION BY campaignId) AS rn
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_brands_campaigns`
  ) 
  WHERE rn = 1
  AND _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at) 
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_campaigns`
  )
) AS source
ON FALSE
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at, 
    name,
    state,
    budget,
    bidOptimization,
    bidOptimizationStrategy,
    endDate,
    costType,
    startDate,
    budgetType,
    campaignId,
    portfolioId,
    smartDefault,
    brandEntityId,
    productLocation
  )
  VALUES (
    source._airbyte_extracted_at, 
    source.name, 
    source.state, 
    source.budget, 
    source.bidOptimization, 
    source.bidOptimizationStrategy, 
    source.endDate, 
    source.costType, 
    source.startDate, 
    source.budgetType, 
    source.campaignId, 
    source.portfolioId, 
    source.smartDefault, 
    source.brandEntityId, 
    source.productLocation
  );
