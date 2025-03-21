CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_normal`
AS
SELECT
    _airbyte_extracted_at,
    cpc,
    cpm,
    cpp,
    ctr,
    ad_id,
    date_start,
    rn,
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
    inline_link_clicks,
    canvas_avg_view_time,
    cost_per_unique_click,
    full_view_impressions,
    inline_link_click_ctr,
    estimated_ad_recallers,
    inline_post_engagement,
    unique_link_clicks_ctr,
    canvas_avg_view_percent,
    conversion_rate_ranking,
    engagement_rate_ranking,
    unique_inline_link_clicks,
    cost_per_inline_link_click,
    unique_inline_link_click_ctr,
    cost_per_estimated_ad_recallers,
    cost_per_inline_post_engagement,
    cost_per_unique_inline_link_click,
    instant_experience_clicks_to_open,
    instant_experience_clicks_to_start,
    qualifying_question_qualify_answer_rate,


    -- purchase_roas,
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
FROM
(
select
*,
row_number() over(partition by ad_id,date_start order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction
)
where rn = 1