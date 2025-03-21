MERGE INTO `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_product_ads` AS target
USING (
  SELECT
    _airbyte_extracted_at,
    sku,
    adId,
    asin,
    state,
    adGroupId,
    campaignId
  FROM (
    SELECT *,
      ROW_NUMBER() OVER(PARTITION BY adId) AS rn
    FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_ads.sponsored_display_product_ads`
  )
  WHERE rn = 1
  AND _airbyte_extracted_at > (
    SELECT MAX(_airbyte_extracted_at)
    FROM `shopify-pubsub-project.Data_Warehouse_Amazon_ads.display_product_ads`
  )
) AS source
ON FALSE 
WHEN NOT MATCHED THEN
  INSERT (
    _airbyte_extracted_at,
    sku,
    adId,
    asin,
    state,
    adGroupId,
    campaignId
  )
  VALUES (
    source._airbyte_extracted_at,
    source.sku,
    source.adId,
    source.asin,
    source.state,
    source.adGroupId,
    source.campaignId
  );
