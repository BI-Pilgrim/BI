MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.profiles` AS target
USING (
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
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.profiles`
  WHERE _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at)
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.profiles`
  )
) AS source
ON FALSE
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    timezone,
    profileId,
    id,
    marketplaceStringId,
    name,
    type,
    validPaymentMethod,
    countryCode,
    dailyBudget,
    currencyCode
  )
  VALUES (
    source._airbyte_extracted_at,
    source.timezone,
    source.profileId,
    source.id,
    source.marketplaceStringId,
    source.name,
    source.type,
    source.validPaymentMethod,
    source.countryCode,
    source.dailyBudget,
    source.currencyCode
  );
