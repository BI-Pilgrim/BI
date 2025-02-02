CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_video_actions`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  date_start,
  JSON_EXTRACT_SCALAR(value1, '$.1d_click') AS one_day_click,
  JSON_EXTRACT_SCALAR(value1, '$.1d_view') AS one_day_view,
  JSON_EXTRACT_SCALAR(value1, '$.28d_click') AS twentyeight_day_click,
  JSON_EXTRACT_SCALAR(value1, '$.28d_view') AS twentyeight_day_view,
  JSON_EXTRACT_SCALAR(value1, '$.7d_click') AS seven_day_click,
  JSON_EXTRACT_SCALAR(value1, '$.7d_view') AS seven_day_view,
  JSON_EXTRACT_SCALAR(value1, '$.action_type') AS action_value_action_type,
  JSON_EXTRACT_SCALAR(value1, '$.value') AS tot_action_value,
  JSON_EXTRACT_SCALAR(unique_action, '$.1d_click') AS unique_action_one_day_click,
  JSON_EXTRACT_SCALAR(unique_action, '$.28d_click') AS unique_action_twentyeight_day_click,
  JSON_EXTRACT_SCALAR(unique_action, '$.7d_click') AS unique_action_seven_day_click,
  JSON_EXTRACT_SCALAR(unique_action, '$.action_destination') AS unique_action_destination,
  JSON_EXTRACT_SCALAR(unique_action, '$.action_target_id') AS unique_action_target_id,
  JSON_EXTRACT_SCALAR(unique_action, '$.action_type') AS unique_action_type,
  JSON_EXTRACT_SCALAR(unique_action, '$.value') AS unique_action_value,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.1d_click') AS vid_play_actions_1d_click,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.1d_view') AS vid_play_actions_1d_view,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.28d_click') AS vid_play_actions_28dclick,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.28d_view') AS vid_play_actions_28d_view,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.7d_click') AS vid_play_actions_7d_click,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.7d_view') AS vid_play_actions_7d_view,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.action_destination') AS vid_play_actions_dextination,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.action_target_id') AS vid_play_actions_target_id,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.action_type') AS vid_play_actions_type,
  JSON_EXTRACT_SCALAR(vid_play_actions, '$.value') AS vid_play_actions_value,
  JSON_EXTRACT(vid_play_curve, '$.value') AS vid_play_curve_actions_value,    
FROM
(
  select
  ,
  row_number() over(partition by ad_id,date_start order by _airbyte_extracted_at desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
  UNNEST(JSON_EXTRACT_ARRAY(video_play_actions)) AS vid_play_actions,
  UNNEST(JSON_EXTRACT_ARRAY(video_play_curve_actions)) AS vid_play_curve,
  UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_action,
  UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS value1
)
where rn = 1

