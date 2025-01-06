CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.videos`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  id,
  icon,
  title,
  views,
  `length`,
  `source`,
  published,
  account_id,
  embed_html,
  embeddable,
  is_episode,
  post_views,
  created_time,
  updated_time,
  permalink_url,
  content_category,
  is_crosspost_video,
  is_instagram_eligible,
  is_crossposting_eligible,


FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.videos
