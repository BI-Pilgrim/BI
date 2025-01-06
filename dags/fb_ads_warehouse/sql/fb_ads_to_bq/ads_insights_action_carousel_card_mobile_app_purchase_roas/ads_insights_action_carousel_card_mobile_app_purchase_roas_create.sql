CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_mobile_app_purchase_roas`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,


  -- mobile_app_purchase_roas,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.28d_click') AS mob_app_purchase_roas_28d_click, --11K(E)
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.28d_view') AS mob_app_purchase_roas_28d_view,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.7d_click') AS mob_app_purchase_roas_7d_click,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.7d_click') AS mob_app_purchase_roas_7d_view,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.1d_click') AS mob_app_purchase_roas_1d_click,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.1d_view') AS mob_app_purchase_roas_1d_view,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.action_type') AS mob_app_purchase_roas_action_type,
  JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.value') AS mob_app_purchase_roas_value,
FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
  UNNEST(JSON_EXTRACT_ARRAY(mobile_app_purchase_roas)) AS mob_app_purchase_roas
