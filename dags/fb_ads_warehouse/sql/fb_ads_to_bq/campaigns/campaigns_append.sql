MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.campaigns` AS TARGET
USING
(
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
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.campaigns
      WHERE
        DATE(_airbyte_extracted_at) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
  WHERE row_num = 1  
) AS SOURCE


ON TARGET.id = SOURCE.id
WHEN MATCHED AND TARGET._airbyte_extracted_at > SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.id = SOURCE.id,
  TARGET.name = SOURCE.name,
  TARGET.status = SOURCE.status,
  TARGET.objective = SOURCE.objective,
  TARGET.stop_time = SOURCE.stop_time,
  TARGET.account_id = SOURCE.account_id,
  TARGET.start_time = SOURCE.start_time,
  TARGET.buying_type = SOURCE.buying_type,
  TARGET.bid_strategy = SOURCE.bid_strategy,
  TARGET.created_time = SOURCE.created_time,
  TARGET.daily_budget = SOURCE.daily_budget,
  TARGET.updated_time = SOURCE.updated_time,
  TARGET.budget_remaining = SOURCE.budget_remaining,
  TARGET.effective_status = SOURCE.effective_status,
  TARGET.configured_status = SOURCE.configured_status,
  TARGET.source_campaign_id = SOURCE.source_campaign_id,
  TARGET.special_ad_category = SOURCE.special_ad_category,
  TARGET.smart_promotion_type = SOURCE.smart_promotion_type,
  TARGET.budget_rebalance_flag = SOURCE.budget_rebalance_flag
WHEN NOT MATCHED
THEN INSERT
(
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
  budget_rebalance_flag
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.id,
  SOURCE.name,
  SOURCE.status,
  SOURCE.objective,
  SOURCE.stop_time,
  SOURCE.account_id,
  SOURCE.start_time,
  SOURCE.buying_type,
  SOURCE.bid_strategy,
  SOURCE.created_time,
  SOURCE.daily_budget,
  SOURCE.updated_time,
  SOURCE.budget_remaining,
  SOURCE.effective_status,
  SOURCE.configured_status,
  SOURCE.source_campaign_id,
  SOURCE.special_ad_category,
  SOURCE.smart_promotion_type,
  SOURCE.budget_rebalance_flag
)
