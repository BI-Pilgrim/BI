CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.label`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  customer_id,
  label_id,
  label_name,
  label_resource_name,
  label_status,
  label_text_label_background_color,
  label_text_label_description,
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.label`
