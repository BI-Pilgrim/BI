MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.video_p25_to_p100_details` AS TARGET
USING
(
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
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY _airbyte_extracted_at ORDER BY _airbyte_extracted_at) AS row_num
    FROM
      shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
      UNNEST(JSON_EXTRACT_ARRAY(video_p100_watched_actions)) AS video_p100,
      UNNEST(JSON_EXTRACT_ARRAY(video_p25_watched_actions)) AS video_p25,
      UNNEST(JSON_EXTRACT_ARRAY(video_p50_watched_actions)) AS video_p50,
      UNNEST(JSON_EXTRACT_ARRAY(video_p75_watched_actions)) AS video_p75,
      UNNEST(JSON_EXTRACT_ARRAY(video_p95_watched_actions)) AS video_p95
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
  TARGET.video_p100_destination = SOURCE.video_p100_destination,
  TARGET.video_p100_target_id = SOURCE.video_p100_target_id,
  TARGET.video_p100_action_value = SOURCE.video_p100_action_value,
  TARGET.video_p25_destination = SOURCE.video_p25_destination,
  TARGET.video_p25_target_id = SOURCE.video_p25_target_id,
  TARGET.video_p25_action_value = SOURCE.video_p25_action_value,
  TARGET.video_p75_destination = SOURCE.video_p75_destination,
  TARGET.video_p75_target_id = SOURCE.video_p75_target_id,
  TARGET.video_p75_action_value = SOURCE.video_p75_action_value,
  TARGET.video_p50_destination = SOURCE.video_p50_destination,
  TARGET.video_p50_target_id = SOURCE.video_p50_target_id,
  TARGET.video_p50_action_value = SOURCE.video_p50_action_value,
  TARGET.video_p95_destination = SOURCE.video_p95_destination,
  TARGET.video_p95_target_id = SOURCE.video_p95_target_id,
  TARGET.video_p95_action_value = SOURCE.video_p95_action_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  video_p100_destination,
  video_p100_target_id,
  video_p100_action_value,
  video_p25_destination,
  video_p25_target_id,
  video_p25_action_value,
  video_p75_destination,
  video_p75_target_id,
  video_p75_action_value,
  video_p50_destination,
  video_p50_target_id,
  video_p50_action_value,
  video_p95_destination,
  video_p95_target_id,
  video_p95_action_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.campaign_id,
  SOURCE.adset_id,
  SOURCE.ad_id,
  SOURCE.video_p100_destination,
  SOURCE.video_p100_target_id,
  SOURCE.video_p100_action_value,
  SOURCE.video_p25_destination,
  SOURCE.video_p25_target_id,
  SOURCE.video_p25_action_value,
  SOURCE.video_p75_destination,
  SOURCE.video_p75_target_id,
  SOURCE.video_p75_action_value,
  SOURCE.video_p50_destination,
  SOURCE.video_p50_target_id,
  SOURCE.video_p50_action_value,
  SOURCE.video_p95_destination,
  SOURCE.video_p95_target_id,
  SOURCE.video_p95_action_value
)
