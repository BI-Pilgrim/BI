CREATE OR REPLACE TABLE
`shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.account_performance_report`
partition by date_trunc(_airbyte_extracted_at, day)
as
select
_airbyte_extracted_at,
customer_auto_tagging_enabled,
customer_id,
customer_manager,
customer_test_account,
metrics_active_view_cpm,
metrics_active_view_ctr,
metrics_active_view_impressions,
metrics_active_view_measurability,
metrics_active_view_measurable_cost_micros,
metrics_active_view_measurable_impressions,
metrics_active_view_viewability,
metrics_all_conversions,
metrics_all_conversions_from_interactions_rate,
metrics_all_conversions_value,
metrics_average_cost,
metrics_average_cpc,
metrics_average_cpe,
metrics_average_cpm,
metrics_average_cpv,
metrics_clicks,
metrics_content_budget_lost_impression_share,
metrics_content_impression_share,
metrics_content_rank_lost_impression_share,
metrics_conversions,
metrics_conversions_from_interactions_rate,
metrics_conversions_value,
metrics_cost_micros,
metrics_cost_per_all_conversions,
metrics_cost_per_conversion,
metrics_cross_device_conversions,
metrics_ctr,
metrics_engagement_rate,
metrics_engagements,
metrics_impressions,
metrics_interaction_rate,
metrics_interactions,
metrics_search_budget_lost_impression_share,
metrics_search_exact_match_impression_share,
metrics_search_impression_share,
metrics_search_rank_lost_impression_share,
metrics_value_per_all_conversions,
metrics_value_per_conversion,
metrics_video_view_rate,
metrics_video_views,
metrics_view_through_conversions,
segments_ad_network_type,
segments_date,
segments_day_of_week,
segments_device,
segments_month,
segments_quarter,
segments_week,
segments_year,
from
shopify-pubsub-project.pilgrim_bi_google_ads.account_performance_report