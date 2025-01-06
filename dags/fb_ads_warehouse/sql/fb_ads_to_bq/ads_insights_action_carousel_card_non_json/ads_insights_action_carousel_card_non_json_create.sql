CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_non_json`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  cpc,
  cpm,
  cpp,
  ctr,
  ad_id,
  reach,
  spend,
  clicks,
  ad_name,
  adset_id,
  date_stop,
  frequency,
  objective,
  account_id,
  adset_name,
  date_start,
  unique_ctr,
  buying_type,
  campaign_id,
  impressions,
  account_name,
  created_time,
  social_spend,
  updated_time,
  campaign_name,
  unique_clicks,
  full_view_reach,
  quality_ranking,
  account_currency,
  optimization_goal,
  canvas_avg_view_time,
  cost_per_unique_click,
  full_view_impressions,
  inline_link_click_ctr,
  estimated_ad_recallers,
  inline_post_engagement,
  canvas_avg_view_percent,
  conversion_rate_ranking,
  engagement_rate_ranking,
  unique_inline_link_clicks,
  cost_per_inline_link_click,
  unique_inline_link_click_ctr,
  cost_per_inline_post_engagement,
  cost_per_unique_inline_link_click,
  instant_experience_clicks_to_open,
  instant_experience_clicks_to_start,
  qualifying_question_qualify_answer_rate,


  -- video_play_curve_actions,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_curve_actions, '$[0]'), '$.action_type') AS video_play_curve_actions_type,
  JSON_EXTRACT(JSON_EXTRACT(video_play_curve_actions, '$[0]'), '$.value') AS video_play_curve_actions_value_array,


  -- video_p95_watched_actions,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p95_watched_actions, '$[0]'), '$.action_type') AS video_p95_watched_actions_type,
  JSON_EXTRACT(JSON_EXTRACT(video_p95_watched_actions, '$[0]'), '$.value') AS video_p95_watched_actions_value,


  -- video_15_sec_watched_actions,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_15_sec_watched_actions, '$[0]'), '$.action_type') AS video_15_sec_watched_actions_type,
  JSON_EXTRACT(JSON_EXTRACT(video_15_sec_watched_actions, '$[0]'), '$.value') AS video_15_sec_watched_actions_value,


  -- video_30_sec_watched_actions,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_30_sec_watched_actions, '$[0]'), '$.action_type') AS video_30_sec_watched_actions_type,
  JSON_EXTRACT(JSON_EXTRACT(video_30_sec_watched_actions, '$[0]'), '$.value') AS video_30_sec_watched_actions_value,


  -- video_avg_time_watched_actions,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_avg_time_watched_actions, '$[0]'), '$.action_carousel_card_id') AS video_avg_time_watched_actions_carousel_card_id,
  JSON_EXTRACT(JSON_EXTRACT(video_avg_time_watched_actions, '$[0]'), '$.action_carousel_card_name') AS video_avg_time_watched_actions_carousel_card_name,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_avg_time_watched_actions, '$[0]'), '$.action_type') AS video_avg_time_watched_actions_type,
  JSON_EXTRACT(JSON_EXTRACT(video_avg_time_watched_actions, '$[0]'), '$.value') AS video_avg_time_watched_actions_value,


  -- video_continuous_2_sec_watched_actions,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_continuous_2_sec_watched_actions, '$[0]'), '$.action_type') AS video_2sec_actions_type,
  JSON_EXTRACT(JSON_EXTRACT(video_continuous_2_sec_watched_actions, '$[0]'), '$.value') AS video_2sec_value,  
FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card
