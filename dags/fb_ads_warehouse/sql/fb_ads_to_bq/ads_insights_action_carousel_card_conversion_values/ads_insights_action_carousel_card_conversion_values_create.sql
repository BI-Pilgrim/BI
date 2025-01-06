CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_conversion_values`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,


  -- conversion_values,
  JSON_EXTRACT_SCALAR(conv_values, '$.1d_click') AS conv_values_1d_click, --400(B)
  JSON_EXTRACT_SCALAR(conv_values, '$.28d_click') AS conv_values_28d_click,
  JSON_EXTRACT_SCALAR(conv_values, '$.7d_click') AS conv_values_7d_click,
  JSON_EXTRACT_SCALAR(conv_values, '$.action_type') AS conv_values_action_type,
  JSON_EXTRACT_SCALAR(conv_values, '$.value') AS conv_values_value,


FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
  UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS conv_values
