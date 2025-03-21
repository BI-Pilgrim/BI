CREATE OR REPLACE TABLE shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_v3_report_stream
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
WITH CTE1 AS (
  SELECT 
    _airbyte_extracted_at,
    recordId, 
    profileId, 
    recordType, 
    reportDate, 
    JSON_EXTRACT_SCALAR(metric, '$.adGroupName') AS adGroupName, 
    JSON_EXTRACT_SCALAR(metric, '$.attributionType') AS attributionType, 
    JSON_EXTRACT_SCALAR(metric, '$.campaignBudgetCurrencyCode') AS campaignBudgetCurrencyCode, 
    JSON_EXTRACT_SCALAR(metric, '$.campaignName') AS campaignName, 
    JSON_EXTRACT_SCALAR(metric, '$.newToBrandPurchases14d') AS newToBrandPurchases14d, 
    JSON_EXTRACT_SCALAR(metric, '$.newToBrandPurchasesPercentage14d') AS newToBrandPurchasesPercentage14d, 
    JSON_EXTRACT_SCALAR(metric, '$.newToBrandSales14d') AS newToBrandSales14d, 
    JSON_EXTRACT_SCALAR(metric, '$.newToBrandSalesPercentage14d') AS newToBrandSalesPercentage14d, 
    JSON_EXTRACT_SCALAR(metric, '$.newToBrandUnitsSold14d') AS newToBrandUnitsSold14d, 
    JSON_EXTRACT_SCALAR(metric, '$.newToBrandUnitsSoldPercentage14d') AS newToBrandUnitsSoldPercentage14d, 
    JSON_EXTRACT_SCALAR(metric, '$.orders14d') AS orders14d, 
    JSON_EXTRACT_SCALAR(metric, '$.productCategory') AS productCategory, 
    JSON_EXTRACT_SCALAR(metric, '$.productName') AS productName, 
    JSON_EXTRACT_SCALAR(metric, '$.purchasedAsin') AS purchasedAsin, 
    JSON_EXTRACT_SCALAR(metric, '$.sales14d') AS sales14d, 
    JSON_EXTRACT_SCALAR(metric, '$.unitsSold14d') AS unitsSold14d,
    ROW_NUMBER() OVER (PARTITION BY recordid,profileid,reportdate)  AS rn
  FROM 
    `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_brands_v3_report_stream`
)
SELECT * 
FROM CTE1 
WHERE rn = 1

