CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_video_2sec_video15_30sec_and_video_avg_details`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  date_start,
  JSON_EXTRACT_SCALAR(video_30_sec, '$.action_destination') AS video_30_sec_action_destination,
  JSON_EXTRACT_SCALAR(video_30_sec, '$.action_target_id') AS video_30_sec_target_id,
  JSON_EXTRACT_SCALAR(video_30_sec, '$.value') AS video_30_sec_value,
  JSON_EXTRACT_SCALAR(video_15_sec, '$.action_destination') AS video_15_sec_action_destination,
  JSON_EXTRACT_SCALAR(video_15_sec, '$.action_target_id') AS video_15_sec_target_id,
  JSON_EXTRACT_SCALAR(video_15_sec, '$.value') AS video_15_sec_value,
  JSON_EXTRACT_SCALAR(video_2_sec, '$.action_destination') AS video_2_sec_action_destination,
  JSON_EXTRACT_SCALAR(video_2_sec, '$.action_target_id') AS video_2_sec_target_id,
  JSON_EXTRACT_SCALAR(video_2_sec, '$.value') AS video_2_sec_value,
  JSON_EXTRACT_SCALAR(video_avg_time, '$.action_destination') AS video_avg_time_destination,
  JSON_EXTRACT_SCALAR(video_avg_time, '$.action_target_id') AS video_avg_time_target_id,
  JSON_EXTRACT_SCALAR(video_avg_time, '$.value') AS video_avg_time_value,  
FROM
(
  select
  *,
  row_number() over(partition by ad_id, date_start order by _airbyte_extracted_at desc) as rn
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
  UNNEST(JSON_EXTRACT_ARRAY(video_30_sec_watched_actions)) AS video_30_sec,
  UNNEST(JSON_EXTRACT_ARRAY(video_15_sec_watched_actions)) AS video_15_sec,
  UNNEST(JSON_EXTRACT_ARRAY(video_continuous_2_sec_watched_actions)) AS video_2_sec,
  UNNEST(JSON_EXTRACT_ARRAY(video_avg_time_watched_actions)) AS video_avg_time
)
where rn = 1