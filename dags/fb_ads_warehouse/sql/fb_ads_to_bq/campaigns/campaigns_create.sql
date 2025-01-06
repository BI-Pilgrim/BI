CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.campaigns`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  id,
  name,
  status,
  objective,
  stop_time,
  account_id,
  start_time,
  buying_type,
  bid_strategy,
  created_time,
  daily_budget,
  updated_time,
  budget_remaining,
  effective_status,
  configured_status,
  source_campaign_id,
  special_ad_category,
  smart_promotion_type,
  budget_rebalance_flag,


FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.campaigns
