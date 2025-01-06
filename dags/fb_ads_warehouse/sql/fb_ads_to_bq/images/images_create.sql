CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.images`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  id,
  url,
  name,
  width,
  height,
  status,
  url_128,
  account_id,
  created_time,
  updated_time,
  permalink_url,
  original_width,
  original_height,
  is_associated_creatives_in_adgroups,


FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.images
