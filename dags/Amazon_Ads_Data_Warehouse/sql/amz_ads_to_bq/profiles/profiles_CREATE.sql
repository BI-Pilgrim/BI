CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.profiles` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS 
SELECT  
_airbyte_extracted_at,
timezone,
profileId,
JSON_EXTRACT_SCALAR(accountInfo, '$.id') AS id,
JSON_EXTRACT_SCALAR(accountInfo, '$.marketplaceStringId') AS marketplaceStringId,
JSON_EXTRACT_SCALAR(accountInfo, '$.name') AS name,
JSON_EXTRACT_SCALAR(accountInfo, '$.type') AS type,
JSON_EXTRACT_SCALAR(accountInfo, '$.validPaymentMethod') AS validPaymentMethod,
countryCode,
dailyBudget,
currencyCode 
FROM (
  SELECT * FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.profiles`
)
