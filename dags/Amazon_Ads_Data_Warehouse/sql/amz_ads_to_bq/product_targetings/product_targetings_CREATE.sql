CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_ads.product_targetings` 
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY) 
AS 
SELECT 
_airbyte_extracted_at,
bid,
state,
targetId,
adGroupId,
campaignId,
JSON_EXTRACT_SCALAR(item, '$.type') AS extracted_type,
JSON_EXTRACT_SCALAR(item, '$.value') AS extracted_value,
expressionType, 
FROM 
(
  SELECT *, 
  ROW_NUMBER() OVER(PARTITION BY `targetid`) AS row_num 
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_product_targetings` ,
  UNNEST(JSON_EXTRACT_ARRAY(resolvedExpression)) AS item
) 
--WHERE row_num = 1