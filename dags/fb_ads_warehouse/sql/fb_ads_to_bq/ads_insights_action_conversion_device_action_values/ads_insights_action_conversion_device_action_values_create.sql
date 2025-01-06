CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_action_values`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at,DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  campaign_id,


  -- action_values,
  JSON_EXTRACT_SCALAR(act_val, '$.1d_click') AS action_values_1d_click,
  JSON_EXTRACT_SCALAR(act_val, '$.1d_view') AS action_values_1d_view,
  JSON_EXTRACT_SCALAR(act_val, '$.28d_click') AS action_values_28d__click,
  JSON_EXTRACT_SCALAR(act_val, '$.28d_click') AS action_values_28d_click,
  JSON_EXTRACT_SCALAR(act_val, '$.7d_click') AS action_values_7d_click,
  JSON_EXTRACT_SCALAR(act_val, '$.7d_view') AS action_values_7d_view,
  JSON_EXTRACT_SCALAR(act_val, '$.action_type') AS action_values_action_type,
  JSON_EXTRACT_SCALAR(act_val, '$.value') AS action_values_value,


FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device,
  UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_val