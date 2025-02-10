CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_bidding_strategy`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
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
  campaign_id,
  customer_id,
  segments_date,
FROM
(
select *,
row_number() over(partition by campaign_id,segments_date order by _airbyte_extracted_at desc) as rn
from `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_bidding_strategy`
)
where rn = 1