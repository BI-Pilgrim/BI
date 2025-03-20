MERGE INTO shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_v3_report_stream AS target
USING (
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
    JSON_EXTRACT_SCALAR(metric, '$.unitsSold14d') AS unitsSold14d
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_brands_v3_report_stream`
  WHERE _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at) 
    FROM shopify-pubsub-project.Data_Warehouse_Amazon_ads.brands_v3_report_stream
  )
) AS source
ON FALSE 
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at, 
    recordId, 
    profileId, 
    recordType, 
    reportDate, 
    adGroupName, 
    attributionType, 
    campaignBudgetCurrencyCode, 
    campaignName, 
    newToBrandPurchases14d, 
    newToBrandPurchasesPercentage14d, 
    newToBrandSales14d, 
    newToBrandSalesPercentage14d, 
    newToBrandUnitsSold14d, 
    newToBrandUnitsSoldPercentage14d, 
    orders14d, 
    productCategory, 
    productName, 
    purchasedAsin, 
    sales14d, 
    unitsSold14d
  )
  VALUES (
    source._airbyte_extracted_at, 
    source.recordId, 
    source.profileId, 
    source.recordType, 
    source.reportDate, 
    source.adGroupName, 
    source.attributionType, 
    source.campaignBudgetCurrencyCode, 
    source.campaignName, 
    source.newToBrandPurchases14d, 
    source.newToBrandPurchasesPercentage14d, 
    source.newToBrandSales14d, 
    source.newToBrandSalesPercentage14d, 
    source.newToBrandUnitsSold14d, 
    source.newToBrandUnitsSoldPercentage14d, 
    source.orders14d, 
    source.productCategory, 
    source.productName, 
    source.purchasedAsin, 
    source.sales14d, 
    source.unitsSold14d
  );
