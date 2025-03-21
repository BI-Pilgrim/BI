MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_campaign` AS target
USING (
  SELECT 
    _airbyte_extracted_at,
    name,
    state,
    JSON_EXTRACT_SCALAR(budget, '$.budget') AS budget,
    JSON_EXTRACT_SCALAR(budget, '$.budgetType') AS budgetType,
    endDate,
    startDate,
    campaignId,
    portfolioId,
    targetingType, 
    placement_bidding.percentage AS placement_percentage,
    placement_bidding.placement AS placement,
    JSON_EXTRACT_SCALAR(dynamicBidding, '$.strategy') AS strategy
  FROM (
    SELECT 
      spc.*, 
      placement_bidding
    FROM 
      `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_campaigns` spc
    LEFT JOIN
      UNNEST(JSON_EXTRACT_ARRAY(spc.dynamicBidding, '$.placementBidding')) AS placement_bidding
  )
  WHERE _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at) 
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_campaign`
  )
) AS source
ON FALSE 
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    name,
    state,
    budget,
    budgetType,
    endDate,
    startDate,
    campaignId,
    portfolioId,
    targetingType,
    placement_percentage,
    placement,
    strategy
  )
  VALUES (
    source._airbyte_extracted_at,
    source.name,
    source.state,
    source.budget,
    source.budgetType,
    source.endDate,
    source.startDate,
    source.campaignId,
    source.portfolioId,
    source.targetingType,
    source.placement_percentage,
    source.placement,
    source.strategy
  );
