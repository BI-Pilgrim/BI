merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_video_p25_to_p100_details` as target
using
(
SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  date_start,
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
  select
  *,
  row_number() over(partition by ad_id, date_start order by _airbyte_extracted_at desc) as rn,
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
  UNNEST(JSON_EXTRACT_ARRAY(video_p100_watched_actions)) AS video_p100,
  UNNEST(JSON_EXTRACT_ARRAY(video_p25_watched_actions)) AS video_p25,
  UNNEST(JSON_EXTRACT_ARRAY(video_p50_watched_actions)) AS video_p50,
  UNNEST(JSON_EXTRACT_ARRAY(video_p75_watched_actions)) AS video_p75,
  UNNEST(JSON_EXTRACT_ARRAY(video_p95_watched_actions)) AS video_p95 
)
where rn = 1  and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
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
target.video_p100_destination = source.video_p100_destination,
target.video_p100_target_id = source.video_p100_target_id,
target.video_p100_action_value = source.video_p100_action_value,
target.video_p25_destination = source.video_p25_destination,
target.video_p25_target_id = source.video_p25_target_id,
target.video_p25_action_value = source.video_p25_action_value,
target.video_p75_destination = source.video_p75_destination,
target.video_p75_target_id = source.video_p75_target_id,
target.video_p75_action_value = source.video_p75_action_value,
target.video_p50_destination = source.video_p50_destination,
target.video_p50_target_id = source.video_p50_target_id,
target.video_p50_action_value = source.video_p50_action_value,
target.video_p95_destination = source.video_p95_destination,
target.video_p95_target_id = source.video_p95_target_id,
target.video_p95_action_value = source.video_p95_action_value
when not matched
then insert
(
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  date_start,
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
values
(
source._airbyte_extracted_at,
source.campaign_id,
source.adset_id,
source.ad_id,
source.date_start,
source.video_p100_destination,
source.video_p100_target_id,
source.video_p100_action_value,
source.video_p25_destination,
source.video_p25_target_id,
source.video_p25_action_value,
source.video_p75_destination,
source.video_p75_target_id,
source.video_p75_action_value,
source.video_p50_destination,
source.video_p50_target_id,
source.video_p50_action_value,
source.video_p95_destination,
source.video_p95_target_id,
source.video_p95_action_value
)