CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,




  -- conversions,
  JSON_EXTRACT_SCALAR(CON1, '$.28d_click') AS conversions_28d_click, --30K(A)
  JSON_EXTRACT_SCALAR(CON1, '$.28d_view') AS conversions_28d_view,
  JSON_EXTRACT_SCALAR(CON1, '$.7d_click') AS conversions_7d_click,
  JSON_EXTRACT_SCALAR(CON1, '$.7d_click') AS conversions_7d_view,
  JSON_EXTRACT_SCALAR(CON1, '$.1d_click') AS conversions_1d_click,
  JSON_EXTRACT_SCALAR(CON1, '$.1d_view') AS conversions_1d_view,
  JSON_EXTRACT_SCALAR(CON1, '$.action_type') AS conversions_action_type,
  JSON_EXTRACT_SCALAR(CON1, '$.value') AS conversions_value,


  -- website_ctr,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(conversions, '$[0]'), '$.action_type') AS action_type, --30K(A)
  CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(conversions, '$[0]'), '$.value') AS FLOAT64) AS value,


  -- unique_actions,
  JSON_EXTRACT_SCALAR(unique_act, '$.1d_click') AS unique_act_1d_click, --30K(A)
  JSON_EXTRACT_SCALAR(unique_act, '$.28d_click') AS unique_act_28d_click,
  JSON_EXTRACT_SCALAR(unique_act, '$.7d_click') AS unique_act_7d_click,
  JSON_EXTRACT_SCALAR(unique_act, '$.action_type') AS unique_act_action_type,
  JSON_EXTRACT_SCALAR(unique_act, '$.value') AS unique_act_value,
FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
  UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS CON1,
  UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_act
