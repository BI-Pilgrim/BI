CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_report_stream`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
WITH CTE1 AS(
SELECT 
  _airbyte_extracted_at,
  recordId, 
  profileId, 
  recordType, 
  reportDate, 
  JSON_EXTRACT_SCALAR(metric, '$.adGroupId') AS adGroupId, 
  JSON_EXTRACT_SCALAR(metric, '$.adGroupName') AS adGroupName,
  JSON_EXTRACT_SCALAR(metric, '$.applicableBudgetRuleId') AS applicableBudgetRuleId,
  JSON_EXTRACT_SCALAR(metric, '$.applicableBudgetRuleName') AS applicableBudgetRuleName,
  JSON_EXTRACT_SCALAR(metric, '$.attributedBrandedSearches14d') AS attributedBrandedSearches14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedConversions14d') AS attributedConversions14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedConversions14dSameSKU') AS attributedConversions14dSameSKU,
  JSON_EXTRACT_SCALAR(metric, '$.attributedDetailPageViewsClicks14d') AS attributedDetailPageViewsClicks14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedOrderRateNewToBrand14d') AS attributedOrderRateNewToBrand14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedOrdersNewToBrand14d') AS attributedOrdersNewToBrand14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedOrdersNewToBrandPercentage14d') AS attributedOrdersNewToBrandPercentage14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedSales14d') AS attributedSales14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedSales14dSameSKU') AS attributedSales14dSameSKU,
  JSON_EXTRACT_SCALAR(metric, '$.attributedSalesNewToBrand14d') AS attributedSalesNewToBrand14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedSalesNewToBrandPercentage14d') AS attributedSalesNewToBrandPercentage14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedUnitsOrderedNewToBrand14d') AS attributedUnitsOrderedNewToBrand14d,
  JSON_EXTRACT_SCALAR(metric, '$.attributedUnitsOrderedNewToBrandPercentage14d') AS attributedUnitsOrderedNewToBrandPercentage14d,
  JSON_EXTRACT_SCALAR(metric, '$.campaignBudget') AS campaignBudget,
  JSON_EXTRACT_SCALAR(metric, '$.campaignBudgetType') AS campaignBudgetType,
  JSON_EXTRACT_SCALAR(metric, '$.campaignId') AS campaignId,
  JSON_EXTRACT_SCALAR(metric, '$.campaignName') AS campaignName,
  JSON_EXTRACT_SCALAR(metric, '$.campaignRuleBasedBudget') AS campaignRuleBasedBudget,
  JSON_EXTRACT_SCALAR(metric, '$.campaignStatus') AS campaignStatus,
  JSON_EXTRACT_SCALAR(metric, '$.clicks') AS clicks,
  JSON_EXTRACT_SCALAR(metric, '$.cost') AS cost,
  JSON_EXTRACT_SCALAR(metric, '$.dpv14d') AS dpv14d,
  JSON_EXTRACT_SCALAR(metric, '$.impressions') AS impressions,
  JSON_EXTRACT_SCALAR(metric, '$.keywordBid') AS keywordBid,
  JSON_EXTRACT_SCALAR(metric, '$.keywordId') AS keywordId,
  JSON_EXTRACT_SCALAR(metric, '$.keywordStatus') AS keywordStatus,
  JSON_EXTRACT_SCALAR(metric, '$.keywordText') AS keywordText,
  JSON_EXTRACT_SCALAR(metric, '$.matchType') AS matchType,
  JSON_EXTRACT_SCALAR(metric, '$.searchTermImpressionRank') AS searchTermImpressionRank,
  JSON_EXTRACT_SCALAR(metric, '$.searchTermImpressionShare') AS searchTermImpressionShare,
  JSON_EXTRACT_SCALAR(metric, '$.unitsSold14d') AS unitsSold14d 
  FROM 
  (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY recordId ORDER BY reportDate ) as rn 
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_brands_report_stream`
  ) 
 ) 
 SELECT * FROM CTE1 
