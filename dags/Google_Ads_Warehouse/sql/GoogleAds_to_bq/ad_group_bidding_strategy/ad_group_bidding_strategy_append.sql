MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_bidding_strategy` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,  
    ad_group_id,
    bidding_strategy_aligned_campaign_budget_id,
    bidding_strategy_campaign_count,
    bidding_strategy_currency_code,
    bidding_strategy_effective_currency_code,
    bidding_strategy_enhanced_cpc,
    bidding_strategy_id,
    bidding_strategy_maximize_conversion_value_cpc_bid_ceiling_micros,
    bidding_strategy_maximize_conversion_value_cpc_bid_floor_micros,
    bidding_strategy_maximize_conversion_value_target_roas,
    bidding_strategy_maximize_conversions_cpc_bid_ceiling_micros,
    bidding_strategy_maximize_conversions_cpc_bid_floor_micros,
    bidding_strategy_maximize_conversions_target_cpa_micros,
    bidding_strategy_name,
    bidding_strategy_non_removed_campaign_count,
    bidding_strategy_resource_name,
    bidding_strategy_status,
    bidding_strategy_target_cpa_cpc_bid_ceiling_micros,
    bidding_strategy_target_cpa_cpc_bid_floor_micros,
    bidding_strategy_target_cpa_target_cpa_micros,
    bidding_strategy_target_impression_share_cpc_bid_ceiling_micros,
    bidding_strategy_target_impression_share_location,
    bidding_strategy_target_impression_share_location_fraction_micros,
    bidding_strategy_target_roas_cpc_bid_ceiling_micros,
    bidding_strategy_target_roas_cpc_bid_floor_micros,
    bidding_strategy_target_roas_target_roas,
    bidding_strategy_target_spend_cpc_bid_ceiling_micros,
    bidding_strategy_target_spend_target_spend_micros,
    bidding_strategy_type,
    segments_date,
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY bidding_strategy_id ORDER BY _airbyte_extracted_at DESC) AS row_num
    FROM
      `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_bidding_strategy`
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1 -- Keep only the most recent row per label_id
) AS SOURCE
ON
  TARGET.bidding_strategy_id = SOURCE.bidding_strategy_id
WHEN
  MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN
  UPDATE SET
    TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,  
    TARGET.ad_group_id = SOURCE.ad_group_id,
    TARGET.bidding_strategy_aligned_campaign_budget_id = SOURCE.bidding_strategy_aligned_campaign_budget_id,
    TARGET.bidding_strategy_campaign_count = SOURCE.bidding_strategy_campaign_count,
    TARGET.bidding_strategy_currency_code = SOURCE.bidding_strategy_currency_code,
    TARGET.bidding_strategy_effective_currency_code = SOURCE.bidding_strategy_effective_currency_code,
    TARGET.bidding_strategy_enhanced_cpc = SOURCE.bidding_strategy_enhanced_cpc,
    TARGET.bidding_strategy_id = SOURCE.bidding_strategy_id,
    TARGET.bidding_strategy_maximize_conversion_value_cpc_bid_ceiling_micros = SOURCE.bidding_strategy_maximize_conversion_value_cpc_bid_ceiling_micros,
    TARGET.bidding_strategy_maximize_conversion_value_cpc_bid_floor_micros = SOURCE.bidding_strategy_maximize_conversion_value_cpc_bid_floor_micros,
    TARGET.bidding_strategy_maximize_conversion_value_target_roas = SOURCE.bidding_strategy_maximize_conversion_value_target_roas,
    TARGET.bidding_strategy_maximize_conversions_cpc_bid_ceiling_micros = SOURCE.bidding_strategy_maximize_conversions_cpc_bid_ceiling_micros,
    TARGET.bidding_strategy_maximize_conversions_cpc_bid_floor_micros = SOURCE.bidding_strategy_maximize_conversions_cpc_bid_floor_micros,
    TARGET.bidding_strategy_maximize_conversions_target_cpa_micros = SOURCE.bidding_strategy_maximize_conversions_target_cpa_micros,
    TARGET.bidding_strategy_name = SOURCE.bidding_strategy_name,
    TARGET.bidding_strategy_non_removed_campaign_count = SOURCE.bidding_strategy_non_removed_campaign_count,
    TARGET.bidding_strategy_resource_name = SOURCE.bidding_strategy_resource_name,
    TARGET.bidding_strategy_status = SOURCE.bidding_strategy_status,
    TARGET.bidding_strategy_target_cpa_cpc_bid_ceiling_micros = SOURCE.bidding_strategy_target_cpa_cpc_bid_ceiling_micros,
    TARGET.bidding_strategy_target_cpa_cpc_bid_floor_micros = SOURCE.bidding_strategy_target_cpa_cpc_bid_floor_micros,
    TARGET.bidding_strategy_target_cpa_target_cpa_micros = SOURCE.bidding_strategy_target_cpa_target_cpa_micros,
    TARGET.bidding_strategy_target_impression_share_cpc_bid_ceiling_micros = SOURCE.bidding_strategy_target_impression_share_cpc_bid_ceiling_micros,
    TARGET.bidding_strategy_target_impression_share_location = SOURCE.bidding_strategy_target_impression_share_location,
    TARGET.bidding_strategy_target_impression_share_location_fraction_micros = SOURCE.bidding_strategy_target_impression_share_location_fraction_micros,
    TARGET.bidding_strategy_target_roas_cpc_bid_ceiling_micros = SOURCE.bidding_strategy_target_roas_cpc_bid_ceiling_micros,
    TARGET.bidding_strategy_target_roas_cpc_bid_floor_micros = SOURCE.bidding_strategy_target_roas_cpc_bid_floor_micros,
    TARGET.bidding_strategy_target_roas_target_roas = SOURCE.bidding_strategy_target_roas_target_roas,
    TARGET.bidding_strategy_target_spend_cpc_bid_ceiling_micros = SOURCE.bidding_strategy_target_spend_cpc_bid_ceiling_micros,
    TARGET.bidding_strategy_target_spend_target_spend_micros = SOURCE.bidding_strategy_target_spend_target_spend_micros,
    TARGET.bidding_strategy_type = SOURCE.bidding_strategy_type,
    TARGET.segments_date = SOURCE.segments_date
WHEN NOT MATCHED
THEN INSERT
(
    _airbyte_extracted_at,  
    ad_group_id,
    bidding_strategy_aligned_campaign_budget_id,
    bidding_strategy_campaign_count,
    bidding_strategy_currency_code,
    bidding_strategy_effective_currency_code,
    bidding_strategy_enhanced_cpc,
    bidding_strategy_id,
    bidding_strategy_maximize_conversion_value_cpc_bid_ceiling_micros,
    bidding_strategy_maximize_conversion_value_cpc_bid_floor_micros,
    bidding_strategy_maximize_conversion_value_target_roas,
    bidding_strategy_maximize_conversions_cpc_bid_ceiling_micros,
    bidding_strategy_maximize_conversions_cpc_bid_floor_micros,
    bidding_strategy_maximize_conversions_target_cpa_micros,
    bidding_strategy_name,
    bidding_strategy_non_removed_campaign_count,
    bidding_strategy_resource_name,
    bidding_strategy_status,
    bidding_strategy_target_cpa_cpc_bid_ceiling_micros,
    bidding_strategy_target_cpa_cpc_bid_floor_micros,
    bidding_strategy_target_cpa_target_cpa_micros,
    bidding_strategy_target_impression_share_cpc_bid_ceiling_micros,
    bidding_strategy_target_impression_share_location,
    bidding_strategy_target_impression_share_location_fraction_micros,
    bidding_strategy_target_roas_cpc_bid_ceiling_micros,
    bidding_strategy_target_roas_cpc_bid_floor_micros,
    bidding_strategy_target_roas_target_roas,
    bidding_strategy_target_spend_cpc_bid_ceiling_micros,
    bidding_strategy_target_spend_target_spend_micros,
    bidding_strategy_type,
    segments_date
)
VALUES
(
  SOURCE._airbyte_extracted_at,  
  SOURCE.ad_group_id,
  SOURCE.bidding_strategy_aligned_campaign_budget_id,
  SOURCE.bidding_strategy_campaign_count,
  SOURCE.bidding_strategy_currency_code,
  SOURCE.bidding_strategy_effective_currency_code,
  SOURCE.bidding_strategy_enhanced_cpc,
  SOURCE.bidding_strategy_id,
  SOURCE.bidding_strategy_maximize_conversion_value_cpc_bid_ceiling_micros,
  SOURCE.bidding_strategy_maximize_conversion_value_cpc_bid_floor_micros,
  SOURCE.bidding_strategy_maximize_conversion_value_target_roas,
  SOURCE.bidding_strategy_maximize_conversions_cpc_bid_ceiling_micros,
  SOURCE.bidding_strategy_maximize_conversions_cpc_bid_floor_micros,
  SOURCE.bidding_strategy_maximize_conversions_target_cpa_micros,
  SOURCE.bidding_strategy_name,
  SOURCE.bidding_strategy_non_removed_campaign_count,
  SOURCE.bidding_strategy_resource_name,
  SOURCE.bidding_strategy_status,
  SOURCE.bidding_strategy_target_cpa_cpc_bid_ceiling_micros,
  SOURCE.bidding_strategy_target_cpa_cpc_bid_floor_micros,
  SOURCE.bidding_strategy_target_cpa_target_cpa_micros,
  SOURCE.bidding_strategy_target_impression_share_cpc_bid_ceiling_micros,
  SOURCE.bidding_strategy_target_impression_share_location,
  SOURCE.bidding_strategy_target_impression_share_location_fraction_micros,
  SOURCE.bidding_strategy_target_roas_cpc_bid_ceiling_micros,
  SOURCE.bidding_strategy_target_roas_cpc_bid_floor_micros,
  SOURCE.bidding_strategy_target_roas_target_roas,
  SOURCE.bidding_strategy_target_spend_cpc_bid_ceiling_micros,
  SOURCE.bidding_strategy_target_spend_target_spend_micros,
  SOURCE.bidding_strategy_type,
  SOURCE.segments_date
);