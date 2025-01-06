CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_cost_per_unique_action_type`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,




  -- cost_per_unique_action_type,
  JSON_EXTRACT_SCALAR(cpuat, '$.1d_click') AS cost_per_unique_action_type_1d_click,
  JSON_EXTRACT_SCALAR(cpuat, '$.1d_view') AS cost_per_unique_action_type_1d_view,
  JSON_EXTRACT_SCALAR(cpuat, '$.28d_click') AS cost_per_unique_action_type_28d_click,
  JSON_EXTRACT_SCALAR(cpuat, '$.28d_views') AS cost_per_unique_action_type_28d_views,  
  JSON_EXTRACT_SCALAR(cpuat, '$.7d_click') AS cost_per_unique_action_type_7d_click,
  JSON_EXTRACT_SCALAR(cpuat, '$.7d_view') AS cost_per_unique_action_type_7d_view,
  JSON_EXTRACT_SCALAR(cpuat, '$.action_type') AS cost_per_unique_action_type_action_type,
  JSON_EXTRACT_SCALAR(cpuat, '$.value') AS cost_per_unique_action_type_value,


FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_platform,
  UNNEST(JSON_EXTRACT_ARRAY(cost_per_unique_action_type)) AS cpuat
