CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_budget`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  campaign_budget_aligned_bidding_strategy_id,
  campaign_budget_amount_micros,
  campaign_budget_delivery_method,
  campaign_budget_explicitly_shared,
  campaign_budget_has_recommended_budget,
  campaign_budget_id,
  campaign_budget_name,
  campaign_budget_period,
  campaign_budget_recommended_budget_amount_micros,
  campaign_budget_recommended_budget_estimated_change_weekly_clicks,
  campaign_budget_recommended_budget_estimated_change_weekly_cost_micros,
  campaign_budget_recommended_budget_estimated_change_weekly_interactions,
  campaign_budget_recommended_budget_estimated_change_weekly_views,
  campaign_budget_reference_count,
  campaign_budget_resource_name,
  campaign_budget_status,
  campaign_budget_total_amount_micros,
  campaign_budget_type,
  campaign_id,
  customer_id,
  metrics_all_conversions,
  metrics_all_conversions_from_interactions_rate,
  metrics_all_conversions_value,
  metrics_average_cost,
  metrics_average_cpc,
  metrics_average_cpe,
  metrics_average_cpm,
  metrics_average_cpv,
  metrics_clicks,
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
  metrics_value_per_all_conversions,
  metrics_value_per_conversion,
  metrics_video_view_rate,
  metrics_video_views,
  metrics_view_through_conversions,
  segments_budget_campaign_association_status_campaign,
  segments_budget_campaign_association_status_status,
    ARRAY_TO_STRING
    (
      ARRAY
      (
        SELECT
          REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(event, '$'), r'InteractionEventType\.(\w+)')
        FROM
          UNNEST(JSON_EXTRACT_ARRAY(metrics_interaction_event_types)) AS event
      ),
      ', '
    ) AS InteractionEventType
FROM
  `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_budget`
