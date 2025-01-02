create or replace table `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.keyword_view`
partition by date_trunc(_airbyte_extracted_at, day)
as
  select
    _airbyte_extracted_at,
    ad_group_criterion_criterion_id,
    ad_group_criterion_keyword_match_type,
    ad_group_criterion_keyword_text,
    ad_group_criterion_negative,
    ad_group_criterion_type,
    ad_group_id,
    campaign_bidding_strategy_type,
    campaign_id,
    customer_descriptive_name,
    customer_id,
    metrics_active_view_impressions,
    metrics_active_view_measurability,
    metrics_active_view_measurable_cost_micros,
    metrics_active_view_measurable_impressions,
    metrics_active_view_viewability,
    metrics_clicks,
    metrics_conversions,
    metrics_conversions_value,
    metrics_cost_micros,
    metrics_ctr,
    metrics_historical_quality_score,
    metrics_impressions,
    metrics_interaction_event_types,
    metrics_interactions,
    metrics_view_through_conversions,
    segments_date,
    JSON_extract_array(metrics_interaction_event_types,'$') as interaction_event_type
  from
    shopify-pubsub-project.pilgrim_bi_google_ads.keyword_view
