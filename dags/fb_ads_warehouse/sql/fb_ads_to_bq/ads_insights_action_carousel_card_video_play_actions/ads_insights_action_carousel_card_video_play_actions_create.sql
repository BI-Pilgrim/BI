CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_video_play_actions`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,


  -- video_play_actions,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.28d_click') AS vid_play_action_28d_click, -- 200(C)
  JSON_EXTRACT_SCALAR(vid_play_action, '$.28d_view') AS vid_play_action_28d_view,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.7d_click') AS vid_play_action_7d_click,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.7d_click') AS vid_play_action_7d_view,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.1d_click') AS vid_play_action_1d_click,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.1d_view') AS vid_play_action_1d_view,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.action_type') AS vid_play_action_action_type,
  JSON_EXTRACT_SCALAR(vid_play_action, '$.value') AS vid_play_action_value,


FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
  UNNEST(JSON_EXTRACT_ARRAY(video_play_actions)) AS vid_play_action
