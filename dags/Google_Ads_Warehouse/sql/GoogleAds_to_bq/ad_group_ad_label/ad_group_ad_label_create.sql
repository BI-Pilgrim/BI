CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_label`
PARTITION BY date_trunc(_airbyte_extracted_at, day)
AS
SELECT
  _airbyte_extracted_at,
  ad_group_ad_ad_id,
  ad_group_ad_ad_resource_name,
  ad_group_ad_label_resource_name,
  ad_group_id,
  label_id,
  label_name,
  label_resource_name
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad_label`;