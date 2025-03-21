MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_campaigns` AS target
USING (
  SELECT
    _airbyte_extracted_at,
    name,
    state,
    budget,
    tactic,
    endDate,
    costType,
    PARSE_DATE('%Y%m%d', startdate) AS Start_Date,
    budgetType,
    campaignId,
    portfolioId,
    deliveryProfile
  FROM 
    `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_display_campaigns`
  WHERE 
    _airbyte_extracted_at > (
      SELECT MAX(_airbyte_extracted_at) 
      FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_campaigns`
    )
) AS source
ON FALSE 
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    name,
    state,
    budget,
    tactic,
    endDate,
    costType,
    Start_Date,
    budgetType,
    campaignId,
    portfolioId,
    deliveryProfile
  )
  VALUES (
    source._airbyte_extracted_at,
    source.name,
    source.state,
    source.budget,
    source.tactic,
    source.endDate,
    source.costType,
    source.Start_Date,
    source.budgetType,
    source.campaignId,
    source.portfolioId,
    source.deliveryProfile
  );
