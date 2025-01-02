CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_label`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, day)
AS
SELECT
_airbyte_extracted_at,
ad_group_id,
ad_group_label_resource_name,
ad_group_resource_name,
label_id,
label_name,
label_resource_name,
FROM
  shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_label;
