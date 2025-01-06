MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.video_actions` AS TARGET
USING
(
  SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
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
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY _airbyte_extracted_at ORDER BY _airbyte_extracted_at) AS row_num
    FROM
      shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
      UNNEST(JSON_EXTRACT_ARRAY(video_play_actions)) AS vid_play_actions,
      UNNEST(JSON_EXTRACT_ARRAY(video_play_curve_actions)) AS vid_play_curve,
      UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_action,
      UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS value1
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1
) AS SOURCE
ON TARGET.adset_id = SOURCE.adset_id
WHEN MATCHED AND TARGET._airbyte_extracted_at > SOURCE._airbyte_extracted_at


THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.adset_id = SOURCE.adset_id,
  TARGET.ad_id = SOURCE.ad_id,
  TARGET.one_day_click = SOURCE.one_day_click,
  TARGET.one_day_view = SOURCE.one_day_view,
  TARGET.twentyeight_day_click = SOURCE.twentyeight_day_click,
  TARGET.twentyeight_day_view = SOURCE.twentyeight_day_view,
  TARGET.seven_day_click = SOURCE.seven_day_click,
  TARGET.seven_day_view = SOURCE.seven_day_view,
  TARGET.action_value_action_type = SOURCE.action_value_action_type,
  TARGET.tot_action_value = SOURCE.tot_action_value,
  TARGET.unique_action_one_day_click = SOURCE.unique_action_one_day_click,
  TARGET.unique_action_twentyeight_day_click = SOURCE.unique_action_twentyeight_day_click,
  TARGET.unique_action_seven_day_click = SOURCE.unique_action_seven_day_click,
  TARGET.unique_action_destination = SOURCE.unique_action_destination,
  TARGET.unique_action_target_id = SOURCE.unique_action_target_id,
  TARGET.unique_action_type = SOURCE.unique_action_type,
  TARGET.unique_action_value = SOURCE.unique_action_value,
  TARGET.vid_play_actions_1d_click = SOURCE.vid_play_actions_1d_click,
  TARGET.vid_play_actions_1d_view = SOURCE.vid_play_actions_1d_view,
  TARGET.vid_play_actions_28dclick = SOURCE.vid_play_actions_28dclick,
  TARGET.vid_play_actions_28d_view = SOURCE.vid_play_actions_28d_view,
  TARGET.vid_play_actions_7d_click = SOURCE.vid_play_actions_7d_click,
  TARGET.vid_play_actions_7d_view = SOURCE.vid_play_actions_7d_view,
  TARGET.vid_play_actions_dextination = SOURCE.vid_play_actions_dextination,
  TARGET.vid_play_actions_target_id = SOURCE.vid_play_actions_target_id,
  TARGET.vid_play_actions_type = SOURCE.vid_play_actions_type,
  TARGET.vid_play_actions_value = SOURCE.vid_play_actions_value,
  TARGET.vid_play_curve_actions_value = SOURCE.vid_play_curve_actions_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  one_day_click,
  one_day_view,
  twentyeight_day_click,
  twentyeight_day_view,
  seven_day_click,
  seven_day_view,
  action_value_action_type,
  tot_action_value,
  unique_action_one_day_click,
  unique_action_twentyeight_day_click,
  unique_action_seven_day_click,
  unique_action_destination,
  unique_action_target_id,
  unique_action_type,
  unique_action_value,
  vid_play_actions_1d_click,
  vid_play_actions_1d_view,
  vid_play_actions_28dclick,
  vid_play_actions_28d_view,
  vid_play_actions_7d_click,
  vid_play_actions_7d_view,
  vid_play_actions_dextination,
  vid_play_actions_target_id,
  vid_play_actions_type,
  vid_play_actions_value,
  vid_play_curve_actions_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.campaign_id,
  SOURCE.adset_id,
  SOURCE.ad_id,
  SOURCE.one_day_click,
  SOURCE.one_day_view,
  SOURCE.twentyeight_day_click,
  SOURCE.twentyeight_day_view,
  SOURCE.seven_day_click,
  SOURCE.seven_day_view,
  SOURCE.action_value_action_type,
  SOURCE.tot_action_value,
  SOURCE.unique_action_one_day_click,
  SOURCE.unique_action_twentyeight_day_click,
  SOURCE.unique_action_seven_day_click,
  SOURCE.unique_action_destination,
  SOURCE.unique_action_target_id,
  SOURCE.unique_action_type,
  SOURCE.unique_action_value,
  SOURCE.vid_play_actions_1d_click,
  SOURCE.vid_play_actions_1d_view,
  SOURCE.vid_play_actions_28dclick,
  SOURCE.vid_play_actions_28d_view,
  SOURCE.vid_play_actions_7d_click,
  SOURCE.vid_play_actions_7d_view,
  SOURCE.vid_play_actions_dextination,
  SOURCE.vid_play_actions_target_id,
  SOURCE.vid_play_actions_type,
  SOURCE.vid_play_actions_value,
  SOURCE.vid_play_curve_actions_value
)
