CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.video_p25_to_p100_details`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  JSON_EXTRACT_SCALAR(video_p100, '$.action_destination') AS video_p100_destination,
  JSON_EXTRACT_SCALAR(video_p100, '$.action_target_id') AS video_p100_target_id,
  JSON_EXTRACT_SCALAR(video_p100, '$.value') AS video_p100_action_value,
  JSON_EXTRACT_SCALAR(video_p25, '$.action_destination') AS video_p25_destination,
  JSON_EXTRACT_SCALAR(video_p25, '$.action_target_id') AS video_p25_target_id,
  JSON_EXTRACT_SCALAR(video_p25, '$.value') AS video_p25_action_value,
  JSON_EXTRACT_SCALAR(video_p75, '$.action_destination') AS video_p75_destination,
  JSON_EXTRACT_SCALAR(video_p75, '$.action_target_id') AS video_p75_target_id,
  JSON_EXTRACT_SCALAR(video_p75, '$.value') AS video_p75_action_value,
  JSON_EXTRACT_SCALAR(video_p50, '$.action_destination') AS video_p50_destination,
  JSON_EXTRACT_SCALAR(video_p50, '$.action_target_id') AS video_p50_target_id,
  JSON_EXTRACT_SCALAR(video_p50, '$.value') AS video_p50_action_value,

  -- video_p95_watched_actions,
  JSON_EXTRACT_SCALAR(video_p95, '$.action_destination') AS video_p95_destination,
  JSON_EXTRACT_SCALAR(video_p95, '$.action_target_id') AS video_p95_target_id,
  JSON_EXTRACT_SCALAR(video_p95, '$.value') AS video_p95_action_value,  
FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
  UNNEST(JSON_EXTRACT_ARRAY(video_p100_watched_actions)) AS video_p100,
  UNNEST(JSON_EXTRACT_ARRAY(video_p25_watched_actions)) AS video_p25,
  UNNEST(JSON_EXTRACT_ARRAY(video_p50_watched_actions)) AS video_p50,
  UNNEST(JSON_EXTRACT_ARRAY(video_p75_watched_actions)) AS video_p75,
  UNNEST(JSON_EXTRACT_ARRAY(video_p95_watched_actions)) AS video_p95 