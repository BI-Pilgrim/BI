merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.video_2sec_video15_30sec_and_video_avg_details` as target
using
(
  select
    _airbyte_extracted_at,
    campaign_id,
    adset_id,
    ad_id,

    -- video_30_sec,
    JSON_EXTRACT_SCALAR(video_30_sec, '$.action_destination') AS video_30_sec_action_destination,
    JSON_EXTRACT_SCALAR(video_30_sec, '$.action_target_id') AS video_30_sec_target_id,
    JSON_EXTRACT_SCALAR(video_30_sec, '$.value') AS video_30_sec_value,

    -- video_15_sec,
    JSON_EXTRACT_SCALAR(video_15_sec, '$.action_destination') AS video_15_sec_action_destination,
    JSON_EXTRACT_SCALAR(video_15_sec, '$.action_target_id') AS video_15_sec_target_id,
    JSON_EXTRACT_SCALAR(video_15_sec, '$.value') AS video_15_sec_value,

    -- video_2_sec,
    JSON_EXTRACT_SCALAR(video_2_sec, '$.action_destination') AS video_2_sec_action_destination,
    JSON_EXTRACT_SCALAR(video_2_sec, '$.action_target_id') AS video_2_sec_target_id,
    JSON_EXTRACT_SCALAR(video_2_sec, '$.value') AS video_2_sec_value,

    -- video_avg_time,
    JSON_EXTRACT_SCALAR(video_avg_time, '$.action_destination') AS video_avg_time_destination,
    JSON_EXTRACT_SCALAR(video_avg_time, '$.action_target_id') AS video_avg_time_target_id,
    JSON_EXTRACT_SCALAR(video_avg_time, '$.value') AS video_avg_time_value, 
  from
  (
    select
    *,
    row_number() over(partition by ad_id order by ad_id) as row_num
    from
    shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
    UNNEST(JSON_EXTRACT_ARRAY(video_30_sec_watched_actions)) AS video_30_sec,
    UNNEST(JSON_EXTRACT_ARRAY(video_15_sec_watched_actions)) AS video_15_sec,
    UNNEST(JSON_EXTRACT_ARRAY(video_continuous_2_sec_watched_actions)) AS video_2_sec,
    UNNEST(JSON_EXTRACT_ARRAY(video_avg_time_watched_actions)) AS video_avg_time   
  )
  where row_num = 1
) as source
on target.ad_id = source.ad_id
when matched and target._airbyte_extracted_at < source._airbyte_extracted_at
then update set
  target._airbyte_extracted_at = source._airbyte_extracted_at,
  target.campaign_id = source.campaign_id,
  target.adset_id = source.adset_id,
  target.ad_id = source.ad_id,
  target.video_30_sec_action_destination = source.video_30_sec_action_destination,
  target.video_30_sec_target_id = source.video_30_sec_target_id,
  target.video_30_sec_value = source.video_30_sec_value,
  target.video_15_sec_action_destination = source.video_15_sec_action_destination,
  target.video_15_sec_target_id = source.video_15_sec_target_id,
  target.video_15_sec_value = source.video_15_sec_value,
  target.video_2_sec_action_destination = source.video_2_sec_action_destination,
  target.video_2_sec_target_id = source.video_2_sec_target_id,
  target.video_2_sec_value = source.video_2_sec_value,
  target.video_avg_time_destination = source.video_avg_time_destination,
  target.video_avg_time_target_id = source.video_avg_time_target_id,
  target.video_avg_time_value = source.video_avg_time_value

when not matched
then insert
(
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  video_30_sec_action_destination,
  video_30_sec_target_id,
  video_30_sec_value,
  video_15_sec_action_destination,
  video_15_sec_target_id,
  video_15_sec_value,
  video_2_sec_action_destination,
  video_2_sec_target_id,
  video_2_sec_value,
  video_avg_time_destination,
  video_avg_time_target_id,
  video_avg_time_value
)
values
(
  source._airbyte_extracted_at,
  source.campaign_id,
  source.adset_id,
  source.ad_id,
  source.video_30_sec_action_destination,
  source.video_30_sec_target_id,
  source.video_30_sec_value,
  source.video_15_sec_action_destination,
  source.video_15_sec_target_id,
  source.video_15_sec_value,
  source.video_2_sec_action_destination,
  source.video_2_sec_target_id,
  source.video_2_sec_value,
  source.video_avg_time_destination,
  source.video_avg_time_target_id,
  source.video_avg_time_value
)