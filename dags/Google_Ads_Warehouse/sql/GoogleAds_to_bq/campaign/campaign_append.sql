MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign` AS TARGET
USING
(
SELECT
_airbyte_extracted_at,
campaign_dynamic_search_ads_setting_use_supplied_urls_only,
campaign_manual_cpc_enhanced_cpc_enabled,
campaign_network_settings_target_content_network,
campaign_network_settings_target_google_search,
campaign_network_settings_target_partner_search_network,
campaign_network_settings_target_search_network,
campaign_percent_cpc_enhanced_cpc_enabled,
campaign_real_time_bidding_setting_opt_in,
campaign_shopping_setting_enable_local,
segments_date,
campaign_budget_amount_micros,
campaign_commission_commission_rate_micros,
campaign_hotel_setting_hotel_center_id,
campaign_id,
campaign_maximize_conversions_target_cpa_micros,
campaign_percent_cpc_cpc_bid_ceiling_micros,
campaign_shopping_setting_campaign_priority,
campaign_shopping_setting_merchant_id,
campaign_target_cpa_cpc_bid_ceiling_micros,
campaign_target_cpa_cpc_bid_floor_micros,
campaign_target_cpa_target_cpa_micros,
campaign_target_cpm_target_frequency_goal_target_count,
campaign_target_impression_share_cpc_bid_ceiling_micros,
campaign_target_impression_share_location_fraction_micros,
campaign_target_roas_cpc_bid_ceiling_micros,
campaign_target_roas_cpc_bid_floor_micros,
campaign_target_spend_cpc_bid_ceiling_micros,
campaign_target_spend_target_spend_micros,
metrics_active_view_impressions,
metrics_active_view_measurable_cost_micros,
metrics_active_view_measurable_impressions,
metrics_clicks,
metrics_cost_micros,
metrics_impressions,
metrics_interactions,
metrics_video_views,
segments_hour,
campaign_maximize_conversion_value_target_roas,
campaign_optimization_score,
campaign_target_roas_target_roas,
metrics_active_view_cpm,
metrics_active_view_ctr,
metrics_active_view_measurability,
metrics_active_view_viewability,
metrics_average_cost,
metrics_average_cpc,
metrics_average_cpm,
metrics_conversions,
metrics_conversions_value,
metrics_cost_per_conversion,
metrics_ctr,
metrics_value_per_conversion,
metrics_video_quartile_p100_rate,
campaign_ad_serving_optimization_status,
campaign_advertising_channel_sub_type,
campaign_advertising_channel_type,
campaign_app_campaign_setting_app_id,
campaign_app_campaign_setting_app_store,
campaign_app_campaign_setting_bidding_strategy_goal_type,
campaign_base_campaign,
campaign_bidding_strategy_type,
campaign_campaign_budget,
campaign_end_date,
campaign_experiment_type,
campaign_geo_target_type_setting_negative_geo_target_type,
campaign_geo_target_type_setting_positive_geo_target_type,
campaign_local_campaign_setting_location_source_type,
campaign_name,
campaign_payment_mode,
campaign_resource_name,
campaign_serving_status,
campaign_start_date,
campaign_status,
campaign_target_impression_share_location,
campaign_tracking_url_template,
campaign_vanity_pharma_vanity_pharma_display_url_mode,
campaign_vanity_pharma_vanity_pharma_text,
campaign_video_brand_safety_suitability,
segments_ad_network_type,
-- -- Extract 'level' value
-- REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(value, '$'), r'level:\s*(\w+)') AS campaign_frequency_cap_level,
-- -- Extract 'event_type' value
-- REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(value, '$'), r'event_type:\s*(\w+)') AS campaign_frequency_cap_event_type,
-- -- Extract 'time_unit' value
-- REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(value, '$'), r'time_unit:\s*(\w+)') AS campaign_frequency_cap_time_unit,
-- -- Extract 'time_length' value
-- REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(value, '$'), r'time_length:\s*(\d+)') AS campaign_frequency_cap_time_length,
-- -- Extract 'cap' value
-- REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(value, '$'), r'cap:\s*(\d+)') AS campaign_frequency_cap_value,
-- Extract campaign label from campaign_label
REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(campaign_labels, '$[0]'),r'customers/\d+/labels/\d+') AS campaign_label,
-- Extract conversion_actions from JSON array campaign_selective_optimization_conversion_actions
JSON_EXTRACT_SCALAR(campaign_selective_optimization_conversion_actions, '$[0]') as campaign_selection_optimization_conv_action,
-- Extract value for dimension from JSON array campaign_targeting_setting_target_restrictions
REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(campaign_targeting_setting_target_restrictions, '$[0]'),r'targeting_dimension:\s*(\w+)') AS campaign_targeting_dimension,
-- Extract value for bid_only from JSON array campaign_targeting_setting_target_restrictions
REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(campaign_targeting_setting_target_restrictions, '$[0]'),r'bid_only:\s*(\w+)') AS campaign_targetting_bid_only,
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY campaign_id ORDER BY campaign_id DESC) AS ROW_NUM
    FROM
      shopify-pubsub-project.pilgrim_bi_google_ads.campaign
  )
  WHERE
    ROW_NUM = 1
) AS SOURCE
ON TARGET.campaign_id = SOURCE.campaign_id
WHEN
  MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN
  UPDATE SET
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.campaign_dynamic_search_ads_setting_use_supplied_urls_only = SOURCE.campaign_dynamic_search_ads_setting_use_supplied_urls_only,
TARGET.campaign_manual_cpc_enhanced_cpc_enabled = SOURCE.campaign_manual_cpc_enhanced_cpc_enabled,
TARGET.campaign_network_settings_target_content_network = SOURCE.campaign_network_settings_target_content_network,
TARGET.campaign_network_settings_target_google_search = SOURCE.campaign_network_settings_target_google_search,
TARGET.campaign_network_settings_target_partner_search_network = SOURCE.campaign_network_settings_target_partner_search_network,
TARGET.campaign_network_settings_target_search_network = SOURCE.campaign_network_settings_target_search_network,
TARGET.campaign_percent_cpc_enhanced_cpc_enabled = SOURCE.campaign_percent_cpc_enhanced_cpc_enabled,
TARGET.campaign_real_time_bidding_setting_opt_in = SOURCE.campaign_real_time_bidding_setting_opt_in,
TARGET.campaign_shopping_setting_enable_local = SOURCE.campaign_shopping_setting_enable_local,
TARGET.segments_date = SOURCE.segments_date,
TARGET.campaign_budget_amount_micros = SOURCE.campaign_budget_amount_micros,
TARGET.campaign_commission_commission_rate_micros = SOURCE.campaign_commission_commission_rate_micros,
TARGET.campaign_hotel_setting_hotel_center_id = SOURCE.campaign_hotel_setting_hotel_center_id,
TARGET.campaign_id = SOURCE.campaign_id,
TARGET.campaign_maximize_conversions_target_cpa_micros = SOURCE.campaign_maximize_conversions_target_cpa_micros,
TARGET.campaign_percent_cpc_cpc_bid_ceiling_micros = SOURCE.campaign_percent_cpc_cpc_bid_ceiling_micros,
TARGET.campaign_shopping_setting_campaign_priority = SOURCE.campaign_shopping_setting_campaign_priority,
TARGET.campaign_shopping_setting_merchant_id = SOURCE.campaign_shopping_setting_merchant_id,
TARGET.campaign_target_cpa_cpc_bid_ceiling_micros = SOURCE.campaign_target_cpa_cpc_bid_ceiling_micros,
TARGET.campaign_target_cpa_cpc_bid_floor_micros = SOURCE.campaign_target_cpa_cpc_bid_floor_micros,
TARGET.campaign_target_cpa_target_cpa_micros = SOURCE.campaign_target_cpa_target_cpa_micros,
TARGET.campaign_target_cpm_target_frequency_goal_target_count = SOURCE.campaign_target_cpm_target_frequency_goal_target_count,
TARGET.campaign_target_impression_share_cpc_bid_ceiling_micros = SOURCE.campaign_target_impression_share_cpc_bid_ceiling_micros,
TARGET.campaign_target_impression_share_location_fraction_micros = SOURCE.campaign_target_impression_share_location_fraction_micros,
TARGET.campaign_target_roas_cpc_bid_ceiling_micros = SOURCE.campaign_target_roas_cpc_bid_ceiling_micros,
TARGET.campaign_target_roas_cpc_bid_floor_micros = SOURCE.campaign_target_roas_cpc_bid_floor_micros,
TARGET.campaign_target_spend_cpc_bid_ceiling_micros = SOURCE.campaign_target_spend_cpc_bid_ceiling_micros,
TARGET.campaign_target_spend_target_spend_micros = SOURCE.campaign_target_spend_target_spend_micros,
TARGET.metrics_active_view_impressions = SOURCE.metrics_active_view_impressions,
TARGET.metrics_active_view_measurable_cost_micros = SOURCE.metrics_active_view_measurable_cost_micros,
TARGET.metrics_active_view_measurable_impressions = SOURCE.metrics_active_view_measurable_impressions,
TARGET.metrics_clicks = SOURCE.metrics_clicks,
TARGET.metrics_cost_micros = SOURCE.metrics_cost_micros,
TARGET.metrics_impressions = SOURCE.metrics_impressions,
TARGET.metrics_interactions = SOURCE.metrics_interactions,
TARGET.metrics_video_views = SOURCE.metrics_video_views,
TARGET.segments_hour = SOURCE.segments_hour,
TARGET.campaign_maximize_conversion_value_target_roas = SOURCE.campaign_maximize_conversion_value_target_roas,
TARGET.campaign_optimization_score = SOURCE.campaign_optimization_score,
TARGET.campaign_target_roas_target_roas = SOURCE.campaign_target_roas_target_roas,
TARGET.metrics_active_view_cpm = SOURCE.metrics_active_view_cpm,
TARGET.metrics_active_view_ctr = SOURCE.metrics_active_view_ctr,
TARGET.metrics_active_view_measurability = SOURCE.metrics_active_view_measurability,
TARGET.metrics_active_view_viewability = SOURCE.metrics_active_view_viewability,
TARGET.metrics_average_cost = SOURCE.metrics_average_cost,
TARGET.metrics_average_cpc = SOURCE.metrics_average_cpc,
TARGET.metrics_average_cpm = SOURCE.metrics_average_cpm,
TARGET.metrics_conversions = SOURCE.metrics_conversions,
TARGET.metrics_conversions_value = SOURCE.metrics_conversions_value,
TARGET.metrics_cost_per_conversion = SOURCE.metrics_cost_per_conversion,
TARGET.metrics_ctr = SOURCE.metrics_ctr,
TARGET.metrics_value_per_conversion = SOURCE.metrics_value_per_conversion,
TARGET.metrics_video_quartile_p100_rate = SOURCE.metrics_video_quartile_p100_rate,
TARGET.campaign_ad_serving_optimization_status = SOURCE.campaign_ad_serving_optimization_status,
TARGET.campaign_advertising_channel_sub_type = SOURCE.campaign_advertising_channel_sub_type,
TARGET.campaign_advertising_channel_type = SOURCE.campaign_advertising_channel_type,
TARGET.campaign_app_campaign_setting_app_id = SOURCE.campaign_app_campaign_setting_app_id,
TARGET.campaign_app_campaign_setting_app_store = SOURCE.campaign_app_campaign_setting_app_store,
TARGET.campaign_app_campaign_setting_bidding_strategy_goal_type = SOURCE.campaign_app_campaign_setting_bidding_strategy_goal_type,
TARGET.campaign_base_campaign = SOURCE.campaign_base_campaign,
TARGET.campaign_bidding_strategy_type = SOURCE.campaign_bidding_strategy_type,
TARGET.campaign_campaign_budget = SOURCE.campaign_campaign_budget,
TARGET.campaign_end_date = SOURCE.campaign_end_date,
TARGET.campaign_experiment_type = SOURCE.campaign_experiment_type,
TARGET.campaign_geo_target_type_setting_negative_geo_target_type = SOURCE.campaign_geo_target_type_setting_negative_geo_target_type,
TARGET.campaign_geo_target_type_setting_positive_geo_target_type = SOURCE.campaign_geo_target_type_setting_positive_geo_target_type,
TARGET.campaign_local_campaign_setting_location_source_type = SOURCE.campaign_local_campaign_setting_location_source_type,
TARGET.campaign_name = SOURCE.campaign_name,
TARGET.campaign_payment_mode = SOURCE.campaign_payment_mode,
TARGET.campaign_resource_name = SOURCE.campaign_resource_name,
TARGET.campaign_serving_status = SOURCE.campaign_serving_status,
TARGET.campaign_start_date = SOURCE.campaign_start_date,
TARGET.campaign_status = SOURCE.campaign_status,
TARGET.campaign_target_impression_share_location = SOURCE.campaign_target_impression_share_location,
TARGET.campaign_tracking_url_template = SOURCE.campaign_tracking_url_template,
TARGET.campaign_vanity_pharma_vanity_pharma_display_url_mode = SOURCE.campaign_vanity_pharma_vanity_pharma_display_url_mode,
TARGET.campaign_vanity_pharma_vanity_pharma_text = SOURCE.campaign_vanity_pharma_vanity_pharma_text,
TARGET.campaign_video_brand_safety_suitability = SOURCE.campaign_video_brand_safety_suitability,
TARGET.segments_ad_network_type = SOURCE.segments_ad_network_type,
TARGET.campaign_label = SOURCE.campaign_label,
TARGET.campaign_selection_optimization_conv_action = SOURCE.campaign_selection_optimization_conv_action,
TARGET.campaign_targeting_dimension = SOURCE.campaign_targeting_dimension,
TARGET.campaign_targetting_bid_only = SOURCE.campaign_targetting_bid_only
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  campaign_dynamic_search_ads_setting_use_supplied_urls_only,
  campaign_manual_cpc_enhanced_cpc_enabled,
  campaign_network_settings_target_content_network,
  campaign_network_settings_target_google_search,
  campaign_network_settings_target_partner_search_network,
  campaign_network_settings_target_search_network,
  campaign_percent_cpc_enhanced_cpc_enabled,
  campaign_real_time_bidding_setting_opt_in,
  campaign_shopping_setting_enable_local,
  segments_date,
  campaign_budget_amount_micros,
  campaign_commission_commission_rate_micros,
  campaign_hotel_setting_hotel_center_id,
  campaign_id,
  campaign_maximize_conversions_target_cpa_micros,
  campaign_percent_cpc_cpc_bid_ceiling_micros,
  campaign_shopping_setting_campaign_priority,
  campaign_shopping_setting_merchant_id,
  campaign_target_cpa_cpc_bid_ceiling_micros,
  campaign_target_cpa_cpc_bid_floor_micros,
  campaign_target_cpa_target_cpa_micros,
  campaign_target_cpm_target_frequency_goal_target_count,
  campaign_target_impression_share_cpc_bid_ceiling_micros,
  campaign_target_impression_share_location_fraction_micros,
  campaign_target_roas_cpc_bid_ceiling_micros,
  campaign_target_roas_cpc_bid_floor_micros,
  campaign_target_spend_cpc_bid_ceiling_micros,
  campaign_target_spend_target_spend_micros,
  metrics_active_view_impressions,
  metrics_active_view_measurable_cost_micros,
  metrics_active_view_measurable_impressions,
  metrics_clicks,
  metrics_cost_micros,
  metrics_impressions,
  metrics_interactions,
  metrics_video_views,
  segments_hour,
  campaign_maximize_conversion_value_target_roas,
  campaign_optimization_score,
  campaign_target_roas_target_roas,
  metrics_active_view_cpm,
  metrics_active_view_ctr,
  metrics_active_view_measurability,
  metrics_active_view_viewability,
  metrics_average_cost,
  metrics_average_cpc,
  metrics_average_cpm,
  metrics_conversions,
  metrics_conversions_value,
  metrics_cost_per_conversion,
  metrics_ctr,
  metrics_value_per_conversion,
  metrics_video_quartile_p100_rate,
  campaign_ad_serving_optimization_status,
  campaign_advertising_channel_sub_type,
  campaign_advertising_channel_type,
  campaign_app_campaign_setting_app_id,
  campaign_app_campaign_setting_app_store,
  campaign_app_campaign_setting_bidding_strategy_goal_type,
  campaign_base_campaign,
  campaign_bidding_strategy_type,
  campaign_campaign_budget,
  campaign_end_date,
  campaign_experiment_type,
  campaign_geo_target_type_setting_negative_geo_target_type,
  campaign_geo_target_type_setting_positive_geo_target_type,
  campaign_local_campaign_setting_location_source_type,
  campaign_name,
  campaign_payment_mode,
  campaign_resource_name,
  campaign_serving_status,
  campaign_start_date,
  campaign_status,
  campaign_target_impression_share_location,
  campaign_tracking_url_template,
  campaign_vanity_pharma_vanity_pharma_display_url_mode,
  campaign_vanity_pharma_vanity_pharma_text,
  campaign_video_brand_safety_suitability,
  segments_ad_network_type,
  campaign_label,
  campaign_selection_optimization_conv_action,
  campaign_targeting_dimension,
  campaign_targetting_bid_only
)
VALUES
(
SOURCE._airbyte_extracted_at,
SOURCE.campaign_dynamic_search_ads_setting_use_supplied_urls_only,
SOURCE.campaign_manual_cpc_enhanced_cpc_enabled,
SOURCE.campaign_network_settings_target_content_network,
SOURCE.campaign_network_settings_target_google_search,
SOURCE.campaign_network_settings_target_partner_search_network,
SOURCE.campaign_network_settings_target_search_network,
SOURCE.campaign_percent_cpc_enhanced_cpc_enabled,
SOURCE.campaign_real_time_bidding_setting_opt_in,
SOURCE.campaign_shopping_setting_enable_local,
SOURCE.segments_date,
SOURCE.campaign_budget_amount_micros,
SOURCE.campaign_commission_commission_rate_micros,
SOURCE.campaign_hotel_setting_hotel_center_id,
SOURCE.campaign_id,
SOURCE.campaign_maximize_conversions_target_cpa_micros,
SOURCE.campaign_percent_cpc_cpc_bid_ceiling_micros,
SOURCE.campaign_shopping_setting_campaign_priority,
SOURCE.campaign_shopping_setting_merchant_id,
SOURCE.campaign_target_cpa_cpc_bid_ceiling_micros,
SOURCE.campaign_target_cpa_cpc_bid_floor_micros,
SOURCE.campaign_target_cpa_target_cpa_micros,
SOURCE.campaign_target_cpm_target_frequency_goal_target_count,
SOURCE.campaign_target_impression_share_cpc_bid_ceiling_micros,
SOURCE.campaign_target_impression_share_location_fraction_micros,
SOURCE.campaign_target_roas_cpc_bid_ceiling_micros,
SOURCE.campaign_target_roas_cpc_bid_floor_micros,
SOURCE.campaign_target_spend_cpc_bid_ceiling_micros,
SOURCE.campaign_target_spend_target_spend_micros,
SOURCE.metrics_active_view_impressions,
SOURCE.metrics_active_view_measurable_cost_micros,
SOURCE.metrics_active_view_measurable_impressions,
SOURCE.metrics_clicks,
SOURCE.metrics_cost_micros,
SOURCE.metrics_impressions,
SOURCE.metrics_interactions,
SOURCE.metrics_video_views,
SOURCE.segments_hour,
SOURCE.campaign_maximize_conversion_value_target_roas,
SOURCE.campaign_optimization_score,
SOURCE.campaign_target_roas_target_roas,
SOURCE.metrics_active_view_cpm,
SOURCE.metrics_active_view_ctr,
SOURCE.metrics_active_view_measurability,
SOURCE.metrics_active_view_viewability,
SOURCE.metrics_average_cost,
SOURCE.metrics_average_cpc,
SOURCE.metrics_average_cpm,
SOURCE.metrics_conversions,
SOURCE.metrics_conversions_value,
SOURCE.metrics_cost_per_conversion,
SOURCE.metrics_ctr,
SOURCE.metrics_value_per_conversion,
SOURCE.metrics_video_quartile_p100_rate,
SOURCE.campaign_ad_serving_optimization_status,
SOURCE.campaign_advertising_channel_sub_type,
SOURCE.campaign_advertising_channel_type,
SOURCE.campaign_app_campaign_setting_app_id,
SOURCE.campaign_app_campaign_setting_app_store,
SOURCE.campaign_app_campaign_setting_bidding_strategy_goal_type,
SOURCE.campaign_base_campaign,
SOURCE.campaign_bidding_strategy_type,
SOURCE.campaign_campaign_budget,
SOURCE.campaign_end_date,
SOURCE.campaign_experiment_type,
SOURCE.campaign_geo_target_type_setting_negative_geo_target_type,
SOURCE.campaign_geo_target_type_setting_positive_geo_target_type,
SOURCE.campaign_local_campaign_setting_location_source_type,
SOURCE.campaign_name,
SOURCE.campaign_payment_mode,
SOURCE.campaign_resource_name,
SOURCE.campaign_serving_status,
SOURCE.campaign_start_date,
SOURCE.campaign_status,
SOURCE.campaign_target_impression_share_location,
SOURCE.campaign_tracking_url_template,
SOURCE.campaign_vanity_pharma_vanity_pharma_display_url_mode,
SOURCE.campaign_vanity_pharma_vanity_pharma_text,
SOURCE.campaign_video_brand_safety_suitability,
SOURCE.segments_ad_network_type,
SOURCE.campaign_label,
SOURCE.campaign_selection_optimization_conv_action,
SOURCE.campaign_targeting_dimension,
SOURCE.campaign_targetting_bid_only
)