merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_normal` as target
using
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
  device_platform,
  full_view_reach,
  account_currency,
  optimization_goal,
  inline_link_clicks,
  canvas_avg_view_time,
  cost_per_unique_click,
  full_view_impressions,
  inline_link_click_ctr,
  estimated_ad_recallers,
  inline_post_engagement,
  unique_link_clicks_ctr,
  canvas_avg_view_percent,
  unique_inline_link_clicks,
  cost_per_inline_link_click,
  unique_inline_link_click_ctr,
  cost_per_estimated_ad_recallers,
  cost_per_inline_post_engagement,
  cost_per_unique_inline_link_click,


  -- actions --UNNEST


  -- website_ctr, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_ctr, '$[0]'), '$.action_type') AS website_ctr_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_ctr, '$[0]'), '$.value') AS website_ctr_value,
 
  -- action_values --UNNEST


  --   -- website_ctr, --(example CODE FOR NORMAL)
  -- JSON_EXTRACT_SCALAR(JSON_EXTRACT(conversions, '$[0]'), '$.action_type') AS action_type, --30K(A)
  -- CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(conversions, '$[0]'), '$.value') AS FLOAT64) AS value,


  -- purchase_roas,-- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.1d_click') AS purchase_roas_1d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.1d_view') AS purchase_roas_1d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.28d_click') AS purchase_roas_28d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.28d_view') AS purchase_roas_28d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.7d_click') AS purchase_roas_7d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.7d_view') AS purchase_roas_7d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.action_type') AS purchase_roas_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(purchase_roas, '$[0]'), '$.value') AS purchase_roas_value,


  -- unique_actions, --UNNEST




  -- outbound_clicks, -- NORMAL


  -- NORMAL TEMPLATE
  -- JSON_EXTRACT_SCALAR(JSON_EXTRACT(, '$[0]'), '$.') AS ,  
 
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks, '$[0]'), '$.action_type') AS outbound_clicks_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks, '$[0]'), '$.value') AS outbound_clicks_value,


  -- cost_per_thruplay, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.1d_click') AS cost_per_thruplay_1d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.1d_view') AS cost_per_thruplay_1d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.28d_click') AS cost_per_thruplay_28d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.28d_view') AS cost_per_thruplay_28d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.7d_click') AS cost_per_thruplay_7d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.7d_view') AS cost_per_thruplay_7d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.action_type') AS cost_per_thruplay_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_thruplay, '$[0]'), '$.value') AS cost_per_thruplay_value,
   
  -- video_play_actions, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.1d_view') AS video_play_actions_1d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.28d_view') AS video_play_actions_28d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.7d_view') AS video_play_actions_7d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.action_type') AS video_play_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_play_actions, '$[0]'), '$.value') AS video_play_actions_value,


  -- outbound_clicks_ctr, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks_ctr, '$[0]'), '$.action_type') AS outbound_clicks_ctr_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks_ctr, '$[0]'), '$.value') AS outbound_clicks_ctr_value,


  -- website_purchase_roas, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.1d_click') AS website_purchase_roas_1d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.1d_view') AS website_purchase_roas_1d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.28d_click') AS website_purchase_roas_28d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.28d_view') AS website_purchase_roas_28d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.7d_click') AS website_purchase_roas_7d_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.7d_view') AS website_purchase_roas_7d_view,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.action_type') AS website_purchase_roas_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_purchase_roas, '$[0]'), '$.value') AS website_purchase_roas_value,


  -- unique_outbound_clicks, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(unique_outbound_clicks, '$[0]'), '$.action_type') AS unique_outbound_clicks_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(unique_outbound_clicks, '$[0]'), '$.value') AS unique_outbound_clicks_value,


  -- cost_per_outbound_click, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_outbound_click, '$[0]'), '$.action_type') AS cost_per_outbound_click_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_outbound_click, '$[0]'), '$.value') AS cost_per_outbound_click_value,


  -- video_p25_watched_actions, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p25_watched_actions, '$[0]'), '$.action_type') AS video_p25_watched_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p25_watched_actions, '$[0]'), '$.value') AS video_p25_watched_actions_value,


  -- video_p50_watched_actions, -- NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p50_watched_actions, '$[0]'), '$.action_type') AS video_p50_watched_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p50_watched_actions, '$[0]'), '$.value') AS video_p50_watched_actions_value,


  -- video_p75_watched_actions,  --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p75_watched_actions, '$[0]'), '$.action_type') AS video_p75_watched_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p75_watched_actions, '$[0]'), '$.value') AS video_p75_watched_actions_value,


  -- video_p95_watched_actions,  --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p95_watched_actions, '$[0]'), '$.action_type') AS video_p95_watched_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p95_watched_actions, '$[0]'), '$.value') AS video_p95_watched_actions_value,


  -- cost_per_15_sec_video_view, --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_15_sec_video_view, '$[0]'), '$.action_type') AS cost_per_15_sec_video_view_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_15_sec_video_view, '$[0]'), '$.value') AS cost_per_15_sec_video_view_value,


  -- unique_outbound_clicks_ctr, --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(unique_outbound_clicks_ctr, '$[0]'), '$.action_type') AS unique_outbound_clicks_ctr_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(unique_outbound_clicks_ctr, '$[0]'), '$.value') AS unique_outbound_clicks_ctr_value,


  -- video_p100_watched_actions, --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p100_watched_actions, '$[0]'), '$.action_type') AS video_p100_watched_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_p100_watched_actions, '$[0]'), '$.value') AS video_p100_watched_actions_value,


  -- -- cost_per_unique_action_type,  --UNNEST
 
  -- video_15_sec_watched_actions, --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_15_sec_watched_actions, '$[0]'), '$.action_type') AS video_15_sec_watched_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_15_sec_watched_actions, '$[0]'), '$.value') AS video_15_sec_watched_actions_value,


  -- video_30_sec_watched_actions, --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_30_sec_watched_actions, '$[0]'), '$.action_type') AS video_30_sec_watched_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_30_sec_watched_actions, '$[0]'), '$.value') AS video_30_sec_watched_actions_value,


  -- cost_per_unique_outbound_click, --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_unique_outbound_click, '$[0]'), '$.action_type') AS cost_per_unique_outbound_click_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_unique_outbound_click, '$[0]'), '$.value') AS cost_per_unique_outbound_click_value,


  -- video_avg_time_watched_actions, --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_avg_time_watched_actions, '$[0]'), '$.action_type') AS video_avg_time_watched_actions_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(video_avg_time_watched_actions, '$[0]'), '$.value') AS video_avg_time_watched_actions_value,


FROM
(
select
*,
row_number() over(partition by ad_id,date_start,device_platform order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device
)
where rn = 1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.device_platform = source.device_platform
when matched and target.date_start < source.date_start
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.cpc = source.cpc,
target.cpm = source.cpm,
target.cpp = source.cpp,
target.ctr = source.ctr,
target.ad_id = source.ad_id,
target.reach = source.reach,
target.spend = source.spend,
target.clicks = source.clicks,
target.ad_name = source.ad_name,
target.adset_id = source.adset_id,
target.date_stop = source.date_stop,
target.frequency = source.frequency,
target.objective = source.objective,
target.account_id = source.account_id,
target.adset_name = source.adset_name,
target.date_start = source.date_start,
target.unique_ctr = source.unique_ctr,
target.buying_type = source.buying_type,
target.campaign_id = source.campaign_id,
target.impressions = source.impressions,
target.account_name = source.account_name,
target.created_time = source.created_time,
target.updated_time = source.updated_time,
target.campaign_name = source.campaign_name,
target.unique_clicks = source.unique_clicks,
target.device_platform = source.device_platform,
target.full_view_reach = source.full_view_reach,
target.account_currency = source.account_currency,
target.optimization_goal = source.optimization_goal,
target.inline_link_clicks = source.inline_link_clicks,
target.canvas_avg_view_time = source.canvas_avg_view_time,
target.cost_per_unique_click = source.cost_per_unique_click,
target.full_view_impressions = source.full_view_impressions,
target.inline_link_click_ctr = source.inline_link_click_ctr,
target.estimated_ad_recallers = source.estimated_ad_recallers,
target.inline_post_engagement = source.inline_post_engagement,
target.unique_link_clicks_ctr = source.unique_link_clicks_ctr,
target.canvas_avg_view_percent = source.canvas_avg_view_percent,
target.unique_inline_link_clicks = source.unique_inline_link_clicks,
target.cost_per_inline_link_click = source.cost_per_inline_link_click,
target.unique_inline_link_click_ctr = source.unique_inline_link_click_ctr,
target.cost_per_estimated_ad_recallers = source.cost_per_estimated_ad_recallers,
target.cost_per_inline_post_engagement = source.cost_per_inline_post_engagement,
target.cost_per_unique_inline_link_click = source.cost_per_unique_inline_link_click,
target.website_ctr_action_type = source.website_ctr_action_type,
target.website_ctr_value = source.website_ctr_value,
target.purchase_roas_1d_click = source.purchase_roas_1d_click,
target.purchase_roas_1d_view = source.purchase_roas_1d_view,
target.purchase_roas_28d_click = source.purchase_roas_28d_click,
target.purchase_roas_28d_view = source.purchase_roas_28d_view,
target.purchase_roas_7d_click = source.purchase_roas_7d_click,
target.purchase_roas_7d_view = source.purchase_roas_7d_view,
target.purchase_roas_action_type = source.purchase_roas_action_type,
target.purchase_roas_value = source.purchase_roas_value,
target.outbound_clicks_action_type = source.outbound_clicks_action_type,
target.outbound_clicks_value = source.outbound_clicks_value,
target.cost_per_thruplay_1d_click = source.cost_per_thruplay_1d_click,
target.cost_per_thruplay_1d_view = source.cost_per_thruplay_1d_view,
target.cost_per_thruplay_28d_click = source.cost_per_thruplay_28d_click,
target.cost_per_thruplay_28d_view = source.cost_per_thruplay_28d_view,
target.cost_per_thruplay_7d_click = source.cost_per_thruplay_7d_click,
target.cost_per_thruplay_7d_view = source.cost_per_thruplay_7d_view,
target.cost_per_thruplay_action_type = source.cost_per_thruplay_action_type,
target.cost_per_thruplay_value = source.cost_per_thruplay_value,
target.video_play_actions_1d_view = source.video_play_actions_1d_view,
target.video_play_actions_28d_view = source.video_play_actions_28d_view,
target.video_play_actions_7d_view = source.video_play_actions_7d_view,
target.video_play_actions_action_type = source.video_play_actions_action_type,
target.video_play_actions_value = source.video_play_actions_value,
target.outbound_clicks_ctr_action_type = source.outbound_clicks_ctr_action_type,
target.outbound_clicks_ctr_value = source.outbound_clicks_ctr_value,
target.website_purchase_roas_1d_click = source.website_purchase_roas_1d_click,
target.website_purchase_roas_1d_view = source.website_purchase_roas_1d_view,
target.website_purchase_roas_28d_click = source.website_purchase_roas_28d_click,
target.website_purchase_roas_28d_view = source.website_purchase_roas_28d_view,
target.website_purchase_roas_7d_click = source.website_purchase_roas_7d_click,
target.website_purchase_roas_7d_view = source.website_purchase_roas_7d_view,
target.website_purchase_roas_action_type = source.website_purchase_roas_action_type,
target.website_purchase_roas_value = source.website_purchase_roas_value,
target.unique_outbound_clicks_action_type = source.unique_outbound_clicks_action_type,
target.unique_outbound_clicks_value = source.unique_outbound_clicks_value,
target.cost_per_outbound_click_action_type = source.cost_per_outbound_click_action_type,
target.cost_per_outbound_click_value = source.cost_per_outbound_click_value,
target.video_p25_watched_actions_action_type = source.video_p25_watched_actions_action_type,
target.video_p25_watched_actions_value = source.video_p25_watched_actions_value,
target.video_p50_watched_actions_action_type = source.video_p50_watched_actions_action_type,
target.video_p50_watched_actions_value = source.video_p50_watched_actions_value,
target.video_p75_watched_actions_action_type = source.video_p75_watched_actions_action_type,
target.video_p75_watched_actions_value = source.video_p75_watched_actions_value,
target.video_p95_watched_actions_action_type = source.video_p95_watched_actions_action_type,
target.video_p95_watched_actions_value = source.video_p95_watched_actions_value,
target.cost_per_15_sec_video_view_action_type = source.cost_per_15_sec_video_view_action_type,
target.cost_per_15_sec_video_view_value = source.cost_per_15_sec_video_view_value,
target.unique_outbound_clicks_ctr_action_type = source.unique_outbound_clicks_ctr_action_type,
target.unique_outbound_clicks_ctr_value = source.unique_outbound_clicks_ctr_value,
target.video_p100_watched_actions_action_type = source.video_p100_watched_actions_action_type,
target.video_p100_watched_actions_value = source.video_p100_watched_actions_value,
target.video_15_sec_watched_actions_action_type = source.video_15_sec_watched_actions_action_type,
target.video_15_sec_watched_actions_value = source.video_15_sec_watched_actions_value,
target.video_30_sec_watched_actions_action_type = source.video_30_sec_watched_actions_action_type,
target.video_30_sec_watched_actions_value = source.video_30_sec_watched_actions_value,
target.cost_per_unique_outbound_click_action_type = source.cost_per_unique_outbound_click_action_type,
target.cost_per_unique_outbound_click_value = source.cost_per_unique_outbound_click_value,
target.video_avg_time_watched_actions_action_type = source.video_avg_time_watched_actions_action_type,
target.video_avg_time_watched_actions_value = source.video_avg_time_watched_actions_value
when not matched
then insert
(
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
  device_platform,
  full_view_reach,
  account_currency,
  optimization_goal,
  inline_link_clicks,
  canvas_avg_view_time,
  cost_per_unique_click,
  full_view_impressions,
  inline_link_click_ctr,
  estimated_ad_recallers,
  inline_post_engagement,
  unique_link_clicks_ctr,
  canvas_avg_view_percent,
  unique_inline_link_clicks,
  cost_per_inline_link_click,
  unique_inline_link_click_ctr,
  cost_per_estimated_ad_recallers,
  cost_per_inline_post_engagement,
  cost_per_unique_inline_link_click,
  website_ctr_action_type,
  website_ctr_value,
  purchase_roas_1d_click,
  purchase_roas_1d_view,
  purchase_roas_28d_click,
  purchase_roas_28d_view,
  purchase_roas_7d_click,
  purchase_roas_7d_view,
  purchase_roas_action_type,
  purchase_roas_value,
  outbound_clicks_action_type,
  outbound_clicks_value,
  cost_per_thruplay_1d_click,
  cost_per_thruplay_1d_view,
  cost_per_thruplay_28d_click,
  cost_per_thruplay_28d_view,
  cost_per_thruplay_7d_click,
  cost_per_thruplay_7d_view,
  cost_per_thruplay_action_type,
  cost_per_thruplay_value,
  video_play_actions_1d_view,
  video_play_actions_28d_view,
  video_play_actions_7d_view,
  video_play_actions_action_type,
  video_play_actions_value,
  outbound_clicks_ctr_action_type,
  outbound_clicks_ctr_value,
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
  cost_per_outbound_click_action_type,
  cost_per_outbound_click_value,
  video_p25_watched_actions_action_type,
  video_p25_watched_actions_value,
  video_p50_watched_actions_action_type,
  video_p50_watched_actions_value,
  video_p75_watched_actions_action_type,
  video_p75_watched_actions_value,
  video_p95_watched_actions_action_type,
  video_p95_watched_actions_value,
  cost_per_15_sec_video_view_action_type,
  cost_per_15_sec_video_view_value,
  unique_outbound_clicks_ctr_action_type,
  unique_outbound_clicks_ctr_value,
  video_p100_watched_actions_action_type,
  video_p100_watched_actions_value,
  video_15_sec_watched_actions_action_type,
  video_15_sec_watched_actions_value,
  video_30_sec_watched_actions_action_type,
  video_30_sec_watched_actions_value,
  cost_per_unique_outbound_click_action_type,
  cost_per_unique_outbound_click_value,
  video_avg_time_watched_actions_action_type,
  video_avg_time_watched_actions_value
)
values
(
source._airbyte_extracted_at,
source.cpc,
source.cpm,
source.cpp,
source.ctr,
source.ad_id,
source.reach,
source.spend,
source.clicks,
source.ad_name,
source.adset_id,
source.date_stop,
source.frequency,
source.objective,
source.account_id,
source.adset_name,
source.date_start,
source.unique_ctr,
source.buying_type,
source.campaign_id,
source.impressions,
source.account_name,
source.created_time,
source.updated_time,
source.campaign_name,
source.unique_clicks,
source.device_platform,
source.full_view_reach,
source.account_currency,
source.optimization_goal,
source.inline_link_clicks,
source.canvas_avg_view_time,
source.cost_per_unique_click,
source.full_view_impressions,
source.inline_link_click_ctr,
source.estimated_ad_recallers,
source.inline_post_engagement,
source.unique_link_clicks_ctr,
source.canvas_avg_view_percent,
source.unique_inline_link_clicks,
source.cost_per_inline_link_click,
source.unique_inline_link_click_ctr,
source.cost_per_estimated_ad_recallers,
source.cost_per_inline_post_engagement,
source.cost_per_unique_inline_link_click,
source.website_ctr_action_type,
source.website_ctr_value,
source.purchase_roas_1d_click,
source.purchase_roas_1d_view,
source.purchase_roas_28d_click,
source.purchase_roas_28d_view,
source.purchase_roas_7d_click,
source.purchase_roas_7d_view,
source.purchase_roas_action_type,
source.purchase_roas_value,
source.outbound_clicks_action_type,
source.outbound_clicks_value,
source.cost_per_thruplay_1d_click,
source.cost_per_thruplay_1d_view,
source.cost_per_thruplay_28d_click,
source.cost_per_thruplay_28d_view,
source.cost_per_thruplay_7d_click,
source.cost_per_thruplay_7d_view,
source.cost_per_thruplay_action_type,
source.cost_per_thruplay_value,
source.video_play_actions_1d_view,
source.video_play_actions_28d_view,
source.video_play_actions_7d_view,
source.video_play_actions_action_type,
source.video_play_actions_value,
source.outbound_clicks_ctr_action_type,
source.outbound_clicks_ctr_value,
source.website_purchase_roas_1d_click,
source.website_purchase_roas_1d_view,
source.website_purchase_roas_28d_click,
source.website_purchase_roas_28d_view,
source.website_purchase_roas_7d_click,
source.website_purchase_roas_7d_view,
source.website_purchase_roas_action_type,
source.website_purchase_roas_value,
source.unique_outbound_clicks_action_type,
source.unique_outbound_clicks_value,
source.cost_per_outbound_click_action_type,
source.cost_per_outbound_click_value,
source.video_p25_watched_actions_action_type,
source.video_p25_watched_actions_value,
source.video_p50_watched_actions_action_type,
source.video_p50_watched_actions_value,
source.video_p75_watched_actions_action_type,
source.video_p75_watched_actions_value,
source.video_p95_watched_actions_action_type,
source.video_p95_watched_actions_value,
source.cost_per_15_sec_video_view_action_type,
source.cost_per_15_sec_video_view_value,
source.unique_outbound_clicks_ctr_action_type,
source.unique_outbound_clicks_ctr_value,
source.video_p100_watched_actions_action_type,
source.video_p100_watched_actions_value,
source.video_15_sec_watched_actions_action_type,
source.video_15_sec_watched_actions_value,
source.video_30_sec_watched_actions_action_type,
source.video_30_sec_watched_actions_value,
source.cost_per_unique_outbound_click_action_type,
source.cost_per_unique_outbound_click_value,
source.video_avg_time_watched_actions_action_type,
source.video_avg_time_watched_actions_value
)