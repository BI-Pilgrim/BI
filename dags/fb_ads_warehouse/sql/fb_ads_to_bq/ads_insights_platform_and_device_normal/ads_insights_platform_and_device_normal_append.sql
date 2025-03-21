MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_platform_and_device_normal` AS TARGET
USING
(
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
    updated_time,
    campaign_name,
    unique_clicks,
    full_view_reach,
    account_currency,
    impression_device,
    optimization_goal,
    platform_position,
    inline_link_clicks,
    publisher_platform,
    canvas_avg_view_time,
    cost_per_unique_click,
    full_view_impressions,
    inline_link_click_ctr,
    inline_post_engagement,
    unique_link_clicks_ctr,
    canvas_avg_view_percent,
    unique_inline_link_clicks,
    cost_per_inline_link_click,
    unique_inline_link_click_ctr,
    cost_per_inline_post_engagement,
    cost_per_unique_inline_link_click,


    -- purchase_roas, -- NORMAL
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.1d_click') AS purchase_roas_1d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.1d_view') AS purchase_roas_1d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.28d_click') AS purchase_roas_28d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.28d_view') AS purchase_roas_28d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.7d_click') AS purchase_roas_7d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.7d_view') AS purchase_roas_7d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.action_type') AS purchase_roas_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.value') AS purchase_roas_value,


    -- website_ctr,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_ctr, '$[0]'), '$.action_type') AS website_ctr_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_ctr, '$[0]'), '$.value') AS website_ctr_value,


    -- outbound_clicks,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks, '$[0]'), '$.action_type') AS outbound_clicks_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks, '$[0]'), '$.value') AS outbound_clicks_value,


    -- video_play_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.1d_click') AS video_play_actions_1d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.1d_view') AS video_play_actions_1d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.28d_click') AS video_play_actions_28d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.28d_view') AS video_play_actions_28d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.7d_click') AS video_play_actions_7d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.7d_view') AS video_play_actions_7d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.action_type') AS video_play_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.value') AS video_play_actions_value,


    -- outbound_clicks_ctr,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks_ctr, '$[0]'), '$.action_type') AS outbound_clicks_ctr_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks_ctr, '$[0]'), '$.value') AS outbound_clicks_ctr_value,






    -- website_purchase_roas,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.1d_click') AS website_purchase_roas_1d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.1d_view') AS website_purchase_roas_1d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.28d_click') AS website_purchase_roas_28d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.28d_view') AS website_purchase_roas_28d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.7d_click') AS website_purchase_roas_7d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.7d_view') AS website_purchase_roas_7d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.action_type') AS website_purchase_roas_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.value') AS website_purchase_roas_value,


    -- unique_outbound_clicks,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(unique_outbound_clicks, '$[0]'), '$.action_type') AS unique_outbound_clicks_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(unique_outbound_clicks, '$[0]'), '$.value') AS unique_outbound_clicks_value,


    -- video_play_curve_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_curve_actions, '$[0]'), '$.action_type') AS video_play_curve_actions_action_type,
    JSON_EXTRACT(JSON_EXTRACT(video_play_curve_actions, '$[0]'), '$.value') AS video_play_curve_actions_value,


    -- video_p25_watched_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p25_watched_actions, '$[0]'), '$.action_type') AS video_p25_watched_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p25_watched_actions, '$[0]'), '$.value') AS video_p25_watched_actions_value,


    -- video_p50_watched_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p50_watched_actions, '$[0]'), '$.action_type') AS video_p50_watched_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p50_watched_actions, '$[0]'), '$.value') AS video_p50_watched_actions_value,


    -- video_p75_watched_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p75_watched_actions, '$[0]'), '$.action_type') AS video_p75_watched_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p75_watched_actions, '$[0]'), '$.value') AS video_p75_watched_actions_value,


    -- video_p95_watched_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p95_watched_actions, '$[0]'), '$.action_type') AS video_p95_watched_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p95_watched_actions, '$[0]'), '$.value') AS video_p95_watched_actions_value,


    -- video_p100_watched_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p95_watched_actions, '$[0]'), '$.action_type') AS video_p100_watched_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p95_watched_actions, '$[0]'), '$.value') AS video_p100_watched_actions_value,


    -- video_15_sec_watched_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_15_sec_watched_actions, '$[0]'), '$.action_type') AS video_15_sec_watched_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_15_sec_watched_actions, '$[0]'), '$.value') AS video_15_sec_watched_actions_value,


    -- video_30_sec_watched_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_30_sec_watched_actions, '$[0]'), '$.action_type') AS video_30_sec_watched_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_30_sec_watched_actions, '$[0]'), '$.value') AS video_30_sec_watched_actions_value,


    -- video_avg_time_watched_actions,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_avg_time_watched_actions, '$[0]'), '$.action_type') AS video_avg_time_watched_actions_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_avg_time_watched_actions, '$[0]'), '$.value') AS video_avg_time_watched_actions_value,


    -- cost_per_thruplay,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.1d_click') AS cost_per_thruplay_1d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.1d_view') AS cost_per_thruplay_1d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.28d_click') AS cost_per_thruplay_28d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.28d_view') AS cost_per_thruplay_28d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.7d_click') AS cost_per_thruplay_7d_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.7d_view') AS cost_per_thruplay_7d_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.action_type') AS cost_per_thruplay_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.value') AS cost_per_thruplay_value,


    -- cost_per_outbound_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_outbound_click, '$[0]'), '$.action_type') AS cost_per_outbound_click_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_outbound_click, '$[0]'), '$.value') AS cost_per_outbound_click_value,


    -- cost_per_15_sec_video_view,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_15_sec_video_view, '$[0]'), '$.action_type') AS cost_per_15_sec_video_view_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_15_sec_video_view, '$[0]'), '$.value') AS cost_per_15_sec_video_view_value,


    -- unique_outbound_clicks_ctr,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(unique_outbound_clicks_ctr, '$[0]'), '$.action_type') AS unique_outbound_clicks_ctr_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(unique_outbound_clicks_ctr, '$[0]'), '$.value') AS unique_outbound_clicks_ctr_value,  


    -- cost_per_unique_outbound_click,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_unique_outbound_click, '$[0]'), '$.action_type') AS cost_per_unique_outbound_click_action_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_unique_outbound_click, '$[0]'), '$.value') AS cost_per_unique_outbound_click_value,


  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_platform_and_device
      WHERE
        DATE(_airbyte_extracted_at) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
  WHERE row_num = 1  
) AS SOURCE


ON TARGET.ad_id = SOURCE.ad_id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
THEN UPDATE SET
    TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
    TARGET.cpc = SOURCE.cpc,
    TARGET.cpm = SOURCE.cpm,
    TARGET.ctr = SOURCE.ctr,
    TARGET.ad_id = SOURCE.ad_id,
    TARGET.reach = SOURCE.reach,
    TARGET.spend = SOURCE.spend,
    TARGET.clicks = SOURCE.clicks,
    TARGET.ad_name = SOURCE.ad_name,
    TARGET.adset_id = SOURCE.adset_id,
    TARGET.date_stop = SOURCE.date_stop,
    TARGET.frequency = SOURCE.frequency,
    TARGET.objective = SOURCE.objective,
    TARGET.account_id = SOURCE.account_id,
    TARGET.adset_name = SOURCE.adset_name,
    TARGET.date_start = SOURCE.date_start,
    TARGET.campaign_id = SOURCE.campaign_id,
    TARGET.impressions = SOURCE.impressions,
    TARGET.account_name = SOURCE.account_name,
    TARGET.created_time = SOURCE.created_time,
    TARGET.updated_time = SOURCE.updated_time,
    TARGET.campaign_name = SOURCE.campaign_name,
    TARGET.unique_clicks = SOURCE.unique_clicks,
    TARGET.account_currency = SOURCE.account_currency,
    TARGET.optimization_goal = SOURCE.optimization_goal,
    TARGET.inline_link_clicks = SOURCE.inline_link_clicks,
    TARGET.inline_post_engagement = SOURCE.inline_post_engagement,
    TARGET.cost_per_inline_link_click = SOURCE.cost_per_inline_link_click,
    TARGET.cost_per_inline_post_engagement = SOURCE.cost_per_inline_post_engagement,
    TARGET.purchase_roas_1d_click = SOURCE.purchase_roas_1d_click,
    TARGET.purchase_roas_1d_view = SOURCE.purchase_roas_1d_view,
    TARGET.purchase_roas_28d_click = SOURCE.purchase_roas_28d_click,
    TARGET.purchase_roas_28d_view = SOURCE.purchase_roas_28d_view,
    TARGET.purchase_roas_7d_click = SOURCE.purchase_roas_7d_click,
    TARGET.purchase_roas_7d_view = SOURCE.purchase_roas_7d_view,
    TARGET.purchase_roas_action_type = SOURCE.purchase_roas_action_type,
    TARGET.purchase_roas_value = SOURCE.purchase_roas_value,
    TARGET.website_ctr_action_type = SOURCE.website_ctr_action_type,
    TARGET.website_ctr_value = SOURCE.website_ctr_value,
    TARGET.outbound_clicks_action_type = SOURCE.outbound_clicks_action_type,
    TARGET.outbound_clicks_value = SOURCE.outbound_clicks_value,
    TARGET.video_play_actions_1d_click = SOURCE.video_play_actions_1d_click,
    TARGET.video_play_actions_1d_view = SOURCE.video_play_actions_1d_view,
    TARGET.video_play_actions_28d_click = SOURCE.video_play_actions_28d_click,
    TARGET.video_play_actions_28d_view = SOURCE.video_play_actions_28d_view,
    TARGET.video_play_actions_7d_click = SOURCE.video_play_actions_7d_click,
    TARGET.video_play_actions_7d_view = SOURCE.video_play_actions_7d_view,
    TARGET.video_play_actions_action_type = SOURCE.video_play_actions_action_type,
    TARGET.video_play_actions_value = SOURCE.video_play_actions_value,
    TARGET.website_purchase_roas_1d_click = SOURCE.website_purchase_roas_1d_click,
    TARGET.website_purchase_roas_1d_view = SOURCE.website_purchase_roas_1d_view,
    TARGET.website_purchase_roas_28d_click = SOURCE.website_purchase_roas_28d_click,
    TARGET.website_purchase_roas_28d_view = SOURCE.website_purchase_roas_28d_view,
    TARGET.website_purchase_roas_7d_click = SOURCE.website_purchase_roas_7d_click,
    TARGET.website_purchase_roas_7d_view = SOURCE.website_purchase_roas_7d_view,
    TARGET.website_purchase_roas_action_type = SOURCE.website_purchase_roas_action_type,
    TARGET.website_purchase_roas_value = SOURCE.website_purchase_roas_value,
    TARGET.unique_outbound_clicks_action_type = SOURCE.unique_outbound_clicks_action_type,
    TARGET.unique_outbound_clicks_value = SOURCE.unique_outbound_clicks_value,
    TARGET.video_play_curve_actions_action_type = SOURCE.video_play_curve_actions_action_type,
    TARGET.video_play_curve_actions_value = SOURCE.video_play_curve_actions_value,
    TARGET.video_p25_watched_actions_action_type = SOURCE.video_p25_watched_actions_action_type,
    TARGET.video_p25_watched_actions_value = SOURCE.video_p25_watched_actions_value,
    TARGET.video_p50_watched_actions_action_type = SOURCE.video_p50_watched_actions_action_type,
    TARGET.video_p50_watched_actions_value = SOURCE.video_p50_watched_actions_value,
    TARGET.video_p75_watched_actions_action_type = SOURCE.video_p75_watched_actions_action_type,
    TARGET.video_p75_watched_actions_value = SOURCE.video_p75_watched_actions_value,
    TARGET.video_p95_watched_actions_action_type = SOURCE.video_p95_watched_actions_action_type,
    TARGET.video_p95_watched_actions_value = SOURCE.video_p95_watched_actions_value,
    TARGET.video_p100_watched_actions_action_type = SOURCE.video_p100_watched_actions_action_type,
    TARGET.video_p100_watched_actions_value = SOURCE.video_p100_watched_actions_value,
    TARGET.video_15_sec_watched_actions_action_type = SOURCE.video_15_sec_watched_actions_action_type,
    TARGET.video_15_sec_watched_actions_value = SOURCE.video_15_sec_watched_actions_value,
    TARGET.video_30_sec_watched_actions_action_type = SOURCE.video_30_sec_watched_actions_action_type,
    TARGET.video_30_sec_watched_actions_value = SOURCE.video_30_sec_watched_actions_value,
    TARGET.video_avg_time_watched_actions_action_type = SOURCE.video_avg_time_watched_actions_action_type,
    TARGET.video_avg_time_watched_actions_value = SOURCE.video_avg_time_watched_actions_value,
    TARGET.cost_per_thruplay_1d_click = SOURCE.cost_per_thruplay_1d_click,
    TARGET.cost_per_thruplay_1d_view = SOURCE.cost_per_thruplay_1d_view,
    TARGET.cost_per_thruplay_28d_click = SOURCE.cost_per_thruplay_28d_click,
    TARGET.cost_per_thruplay_28d_view = SOURCE.cost_per_thruplay_28d_view,
    TARGET.cost_per_thruplay_7d_click = SOURCE.cost_per_thruplay_7d_click,
    TARGET.cost_per_thruplay_7d_view = SOURCE.cost_per_thruplay_7d_view,
    TARGET.cost_per_thruplay_action_type = SOURCE.cost_per_thruplay_action_type,
    TARGET.cost_per_thruplay_value = SOURCE.cost_per_thruplay_value,
    TARGET.cost_per_outbound_click_action_type = SOURCE.cost_per_outbound_click_action_type,
    TARGET.cost_per_outbound_click_value = SOURCE.cost_per_outbound_click_value,
    TARGET.cost_per_15_sec_video_view_action_type = SOURCE.cost_per_15_sec_video_view_action_type,
    TARGET.cost_per_15_sec_video_view_value = SOURCE.cost_per_15_sec_video_view_value,
    TARGET.cost_per_unique_outbound_click_action_type = SOURCE.cost_per_unique_outbound_click_action_type,
    TARGET.cost_per_unique_outbound_click_value = SOURCE.cost_per_unique_outbound_click_value,
    TARGET.outbound_clicks_ctr_action_type = SOURCE.outbound_clicks_ctr_action_type,
    TARGET.outbound_clicks_ctr_value = SOURCE.outbound_clicks_ctr_value,
    TARGET.unique_outbound_clicks_ctr_action_type = SOURCE.unique_outbound_clicks_ctr_action_type,
    TARGET.unique_outbound_clicks_ctr_value = SOURCE.unique_outbound_clicks_ctr_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  cpc,
  cpm,
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
  campaign_id,
  impressions,
  account_name,
  created_time,
  updated_time,
  campaign_name,
  unique_clicks,
  account_currency,
  optimization_goal,
  inline_link_clicks,
  inline_post_engagement,
  cost_per_inline_link_click,
  cost_per_inline_post_engagement,
  purchase_roas_1d_click,
  purchase_roas_1d_view,
  purchase_roas_28d_click,
  purchase_roas_28d_view,
  purchase_roas_7d_click,
  purchase_roas_7d_view,
  purchase_roas_action_type,
  purchase_roas_value,
  website_ctr_action_type,
  website_ctr_value,
  outbound_clicks_action_type,
  outbound_clicks_value,
  video_play_actions_1d_click,
  video_play_actions_1d_view,
  video_play_actions_28d_click,
  video_play_actions_28d_view,
  video_play_actions_7d_click,
  video_play_actions_7d_view,
  video_play_actions_action_type,
  video_play_actions_value,
  website_purchase_roas_1d_click,
  website_purchase_roas_1d_view,
  website_purchase_roas_28d_click,
  website_purchase_roas_28d_view,
  website_purchase_roas_7d_click,
  website_purchase_roas_7d_view,
  website_purchase_roas_action_type,
  website_purchase_roas_value,
  unique_outbound_clicks_action_type,
  unique_outbound_clicks_value,
  video_play_curve_actions_action_type,
  video_play_curve_actions_value,
  video_p25_watched_actions_action_type,
  video_p25_watched_actions_value,
  video_p50_watched_actions_action_type,
  video_p50_watched_actions_value,
  video_p75_watched_actions_action_type,
  video_p75_watched_actions_value,
  video_p95_watched_actions_action_type,
  video_p95_watched_actions_value,
  video_p100_watched_actions_action_type,
  video_p100_watched_actions_value,
  video_15_sec_watched_actions_action_type,
  video_15_sec_watched_actions_value,
  video_30_sec_watched_actions_action_type,
  video_30_sec_watched_actions_value,
  video_avg_time_watched_actions_action_type,
  video_avg_time_watched_actions_value,
  cost_per_thruplay_1d_click,
  cost_per_thruplay_1d_view,
  cost_per_thruplay_28d_click,
  cost_per_thruplay_28d_view,
  cost_per_thruplay_7d_click,
  cost_per_thruplay_7d_view,
  cost_per_thruplay_action_type,
  cost_per_thruplay_value,
  cost_per_outbound_click_action_type,
  cost_per_outbound_click_value,
  cost_per_15_sec_video_view_action_type,
  cost_per_15_sec_video_view_value,
  cost_per_unique_outbound_click_action_type,
  cost_per_unique_outbound_click_value,
  outbound_clicks_ctr_action_type,
  outbound_clicks_ctr_value,
  unique_outbound_clicks_ctr_action_type,
  unique_outbound_clicks_ctr_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.cpc,
  SOURCE.cpm,
  SOURCE.ctr,
  SOURCE.ad_id,
  SOURCE.reach,
  SOURCE.spend,
  SOURCE.clicks,
  SOURCE.ad_name,
  SOURCE.adset_id,
  SOURCE.date_stop,
  SOURCE.frequency,
  SOURCE.objective,
  SOURCE.account_id,
  SOURCE.adset_name,
  SOURCE.date_start,
  SOURCE.campaign_id,
  SOURCE.impressions,
  SOURCE.account_name,
  SOURCE.created_time,
  SOURCE.updated_time,
  SOURCE.campaign_name,
  SOURCE.unique_clicks,
  SOURCE.account_currency,
  SOURCE.optimization_goal,
  SOURCE.inline_link_clicks,
  SOURCE.inline_post_engagement,
  SOURCE.cost_per_inline_link_click,
  SOURCE.cost_per_inline_post_engagement,
  SOURCE.purchase_roas_1d_click,
  SOURCE.purchase_roas_1d_view,
  SOURCE.purchase_roas_28d_click,
  SOURCE.purchase_roas_28d_view,
  SOURCE.purchase_roas_7d_click,
  SOURCE.purchase_roas_7d_view,
  SOURCE.purchase_roas_action_type,
  SOURCE.purchase_roas_value,
  SOURCE.website_ctr_action_type,
  SOURCE.website_ctr_value,
  SOURCE.outbound_clicks_action_type,
  SOURCE.outbound_clicks_value,
  SOURCE.video_play_actions_1d_click,
  SOURCE.video_play_actions_1d_view,
  SOURCE.video_play_actions_28d_click,
  SOURCE.video_play_actions_28d_view,
  SOURCE.video_play_actions_7d_click,
  SOURCE.video_play_actions_7d_view,
  SOURCE.video_play_actions_action_type,
  SOURCE.video_play_actions_value,
  SOURCE.website_purchase_roas_1d_click,
  SOURCE.website_purchase_roas_1d_view,
  SOURCE.website_purchase_roas_28d_click,
  SOURCE.website_purchase_roas_28d_view,
  SOURCE.website_purchase_roas_7d_click,
  SOURCE.website_purchase_roas_7d_view,
  SOURCE.website_purchase_roas_action_type,
  SOURCE.website_purchase_roas_value,
  SOURCE.unique_outbound_clicks_action_type,
  SOURCE.unique_outbound_clicks_value,
  SOURCE.video_play_curve_actions_action_type,
  SOURCE.video_play_curve_actions_value,
  SOURCE.video_p25_watched_actions_action_type,
  SOURCE.video_p25_watched_actions_value,
  SOURCE.video_p50_watched_actions_action_type,
  SOURCE.video_p50_watched_actions_value,
  SOURCE.video_p75_watched_actions_action_type,
  SOURCE.video_p75_watched_actions_value,
  SOURCE.video_p95_watched_actions_action_type,
  SOURCE.video_p95_watched_actions_value,
  SOURCE.video_p100_watched_actions_action_type,
  SOURCE.video_p100_watched_actions_value,
  SOURCE.video_15_sec_watched_actions_action_type,
  SOURCE.video_15_sec_watched_actions_value,
  SOURCE.video_30_sec_watched_actions_action_type,
  SOURCE.video_30_sec_watched_actions_value,
  SOURCE.video_avg_time_watched_actions_action_type,
  SOURCE.video_avg_time_watched_actions_value,
  SOURCE.cost_per_thruplay_1d_click,
  SOURCE.cost_per_thruplay_1d_view,
  SOURCE.cost_per_thruplay_28d_click,
  SOURCE.cost_per_thruplay_28d_view,
  SOURCE.cost_per_thruplay_7d_click,
  SOURCE.cost_per_thruplay_7d_view,
  SOURCE.cost_per_thruplay_action_type,
  SOURCE.cost_per_thruplay_value,
  SOURCE.cost_per_outbound_click_action_type,
  SOURCE.cost_per_outbound_click_value,
  SOURCE.cost_per_15_sec_video_view_action_type,
  SOURCE.cost_per_15_sec_video_view_value,
  SOURCE.cost_per_unique_outbound_click_action_type,
  SOURCE.cost_per_unique_outbound_click_value,
  SOURCE.outbound_clicks_ctr_action_type,
  SOURCE.outbound_clicks_ctr_value,
  SOURCE.unique_outbound_clicks_ctr_action_type,
  SOURCE.unique_outbound_clicks_ctr_value
)
