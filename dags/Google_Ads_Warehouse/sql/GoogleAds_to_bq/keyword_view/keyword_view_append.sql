MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.keyword_view` AS TARGET
USING
(
  SELECT
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
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY ad_group_criterion_criterion_id ORDER BY ad_group_criterion_criterion_id DESC) AS row_num
    FROM
      `shopify-pubsub-project.pilgrim_bi_google_ads.keyword_view`
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1 -- Keep only the most recent row per label_id
) AS SOURCE
ON
  TARGET.ad_group_criterion_criterion_id = SOURCE.ad_group_criterion_criterion_id
WHEN
  MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN
  UPDATE SET
    TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
    TARGET.ad_group_criterion_criterion_id = SOURCE.ad_group_criterion_criterion_id,
    TARGET.ad_group_criterion_keyword_match_type = SOURCE.ad_group_criterion_keyword_match_type,
    TARGET.ad_group_criterion_keyword_text = SOURCE.ad_group_criterion_keyword_text,
    TARGET.ad_group_criterion_negative = SOURCE.ad_group_criterion_negative,
    TARGET.ad_group_criterion_type = SOURCE.ad_group_criterion_type,
    TARGET.ad_group_id = SOURCE.ad_group_id,
    TARGET.campaign_bidding_strategy_type = SOURCE.campaign_bidding_strategy_type,
    TARGET.campaign_id = SOURCE.campaign_id,
    TARGET.customer_descriptive_name = SOURCE.customer_descriptive_name,
    TARGET.customer_id = SOURCE.customer_id,
    TARGET.metrics_active_view_impressions = SOURCE.metrics_active_view_impressions,
    TARGET.metrics_active_view_measurability = SOURCE.metrics_active_view_measurability,
    TARGET.metrics_active_view_measurable_cost_micros = SOURCE.metrics_active_view_measurable_cost_micros,
    TARGET.metrics_active_view_measurable_impressions = SOURCE.metrics_active_view_measurable_impressions,
    TARGET.metrics_active_view_viewability = SOURCE.metrics_active_view_viewability,
    TARGET.metrics_clicks = SOURCE.metrics_clicks,
    TARGET.metrics_conversions = SOURCE.metrics_conversions,
    TARGET.metrics_conversions_value = SOURCE.metrics_conversions_value,
    TARGET.metrics_cost_micros = SOURCE.metrics_cost_micros,
    TARGET.metrics_ctr = SOURCE.metrics_ctr,
    TARGET.metrics_historical_quality_score = SOURCE.metrics_historical_quality_score,
    TARGET.metrics_impressions = SOURCE.metrics_impressions,
    TARGET.metrics_interaction_event_types = SOURCE.metrics_interaction_event_types,
    TARGET.metrics_interactions = SOURCE.metrics_interactions,
    TARGET.metrics_view_through_conversions = SOURCE.metrics_view_through_conversions,
    TARGET.segments_date = SOURCE.segments_date,
    TARGET.interaction_event_type = SOURCE.interaction_event_type
WHEN NOT MATCHED
THEN INSERT
(
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
  interaction_event_type
)
VALUES
(
  source._airbyte_extracted_at,
  SOURCE.ad_group_criterion_criterion_id,
  SOURCE.ad_group_criterion_keyword_match_type,
  SOURCE.ad_group_criterion_keyword_text,
  SOURCE.ad_group_criterion_negative,
  SOURCE.ad_group_criterion_type,
  SOURCE.ad_group_id,
  SOURCE.campaign_bidding_strategy_type,
  SOURCE.campaign_id,
  SOURCE.customer_descriptive_name,
  SOURCE.customer_id,
  SOURCE.metrics_active_view_impressions,
  SOURCE.metrics_active_view_measurability,
  SOURCE.metrics_active_view_measurable_cost_micros,
  SOURCE.metrics_active_view_measurable_impressions,
  SOURCE.metrics_active_view_viewability,
  SOURCE.metrics_clicks,
  SOURCE.metrics_conversions,
  SOURCE.metrics_conversions_value,
  SOURCE.metrics_cost_micros,
  SOURCE.metrics_ctr,
  SOURCE.metrics_historical_quality_score,
  SOURCE.metrics_impressions,
  SOURCE.metrics_interaction_event_types,
  SOURCE.metrics_interactions,
  SOURCE.metrics_view_through_conversions,
  SOURCE.segments_date,
  SOURCE.interaction_event_type
);
