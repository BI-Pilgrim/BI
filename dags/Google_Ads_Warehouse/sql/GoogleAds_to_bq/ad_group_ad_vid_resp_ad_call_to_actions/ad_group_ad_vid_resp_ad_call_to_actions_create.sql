CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_ad_vid_resp_ad_call_to_actions`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  ad_group_ad_ad_id,
  ad_group_id,
  _airbyte_extracted_at,

  -- ad_group_ad_ad_video_responsive_ad_call_to_actions,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(action, '$'), r'text: \"([^\"]+)\"') AS video_responsive_ad_call_to_actions
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_ad`,
  UNNEST(JSON_EXTRACT_ARRAY(ad_group_ad_ad_video_responsive_ad_call_to_actions)) AS action;
