merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_video_actions` as target
using
(
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
  *,
  row_number() over(partition by ad_id,date_start order by _airbyte_extracted_at desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
  UNNEST(JSON_EXTRACT_ARRAY(video_play_actions)) AS vid_play_actions,
  UNNEST(JSON_EXTRACT_ARRAY(video_play_curve_actions)) AS vid_play_curve,
  UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_action,
  UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS value1
)
where rn = 1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
when matched and target._airbyte_extracted_at < source._airbyte_extracted_at
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.campaign_id = source.campaign_id,
target.adset_id = source.adset_id,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.one_day_click = source.one_day_click,
target.one_day_view = source.one_day_view,
target.twentyeight_day_click = source.twentyeight_day_click,
target.twentyeight_day_view = source.twentyeight_day_view,
target.seven_day_click = source.seven_day_click,
target.seven_day_view = source.seven_day_view,
target.action_value_action_type = source.action_value_action_type,
target.tot_action_value = source.tot_action_value,
target.unique_action_one_day_click = source.unique_action_one_day_click,
target.unique_action_twentyeight_day_click = source.unique_action_twentyeight_day_click,
target.unique_action_seven_day_click = source.unique_action_seven_day_click,
target.unique_action_destination = source.unique_action_destination,
target.unique_action_target_id = source.unique_action_target_id,
target.unique_action_type = source.unique_action_type,
target.unique_action_value = source.unique_action_value,
target.vid_play_actions_1d_click = source.vid_play_actions_1d_click,
target.vid_play_actions_1d_view = source.vid_play_actions_1d_view,
target.vid_play_actions_28dclick = source.vid_play_actions_28dclick,
target.vid_play_actions_28d_view = source.vid_play_actions_28d_view,
target.vid_play_actions_7d_click = source.vid_play_actions_7d_click,
target.vid_play_actions_7d_view = source.vid_play_actions_7d_view,
target.vid_play_actions_dextination = source.vid_play_actions_dextination,
target.vid_play_actions_target_id = source.vid_play_actions_target_id,
target.vid_play_actions_type = source.vid_play_actions_type,
target.vid_play_actions_value = source.vid_play_actions_value,
target.vid_play_curve_actions_value = source.vid_play_curve_actions_value
when not matched
then insert
(
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  date_start,
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
values
(
source._airbyte_extracted_at,
source.campaign_id,
source.adset_id,
source.ad_id,
source.date_start,
source.one_day_click,
source.one_day_view,
source.twentyeight_day_click,
source.twentyeight_day_view,
source.seven_day_click,
source.seven_day_view,
source.action_value_action_type,
source.tot_action_value,
source.unique_action_one_day_click,
source.unique_action_twentyeight_day_click,
source.unique_action_seven_day_click,
source.unique_action_destination,
source.unique_action_target_id,
source.unique_action_type,
source.unique_action_value,
source.vid_play_actions_1d_click,
source.vid_play_actions_1d_view,
source.vid_play_actions_28dclick,
source.vid_play_actions_28d_view,
source.vid_play_actions_7d_click,
source.vid_play_actions_7d_view,
source.vid_play_actions_dextination,
source.vid_play_actions_target_id,
source.vid_play_actions_type,
source.vid_play_actions_value,
source.vid_play_curve_actions_value
)