CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.roas`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  JSON_EXTRACT_SCALAR(roas, '$.1d_click') AS one_day_click_roas,
  JSON_EXTRACT_SCALAR(roas, '$.1d_view') AS one_day_view_roas,
  JSON_EXTRACT_SCALAR(roas, '$.28d_click') AS twentyeight_day_click_roas,
  JSON_EXTRACT_SCALAR(roas, '$.28d_view') AS twentyeight_day_view_roas,
  JSON_EXTRACT_SCALAR(roas, '$.7d_click') AS seven_day_click_roas,
  JSON_EXTRACT_SCALAR(roas, '$.7d_view') AS seven_day_view_roas,
  JSON_EXTRACT_SCALAR(roas, '$.action_type') AS roas_action_type,
  JSON_EXTRACT_SCALAR(roas, '$.value') AS value_roas,
  JSON_EXTRACT_SCALAR(p_roas, '$.1d_click') AS one_day_click_purchase_roas,
  JSON_EXTRACT_SCALAR(p_roas, '$.1d_view') AS one_day_view_purchase_roas,
  JSON_EXTRACT_SCALAR(p_roas, '$.28d_click') AS twentyeight_day_click_purchase_roas,
  JSON_EXTRACT_SCALAR(p_roas, '$.28d_view') AS twentyeight_day_view_purchase_roas,
  JSON_EXTRACT_SCALAR(p_roas, '$.7d_click') AS seven_day_click_purchase_roas,
  JSON_EXTRACT_SCALAR(p_roas, '$.7d_view') AS seven_day_view_purchase_roas,
  JSON_EXTRACT_SCALAR(p_roas, '$.action_type') AS purchase_roas_action_type,
  JSON_EXTRACT_SCALAR(p_roas, '$.value') AS value_purchase_roas,  
  JSON_EXTRACT_SCALAR(web_purchase_roas, '$.1d_click') AS web_purchase_roas_1d_click,
  JSON_EXTRACT_SCALAR(web_purchase_roas, '$.1d_view') AS web_purchase_roas_1d_view,
  JSON_EXTRACT_SCALAR(web_purchase_roas, '$.28d_click') AS web_purchase_roas_28d_click,
  JSON_EXTRACT_SCALAR(web_purchase_roas, '$.28d_view') AS web_purchase_roas_28d_view,
  JSON_EXTRACT_SCALAR(web_purchase_roas, '$.7d_click') AS web_purchase_roas_7d_click,
  JSON_EXTRACT_SCALAR(web_purchase_roas, '$.7d_view') AS web_purchase_roas_7d_view,
  JSON_EXTRACT_SCALAR(web_purchase_roas, '$.action_type') AS web_purchase_roas_action_type,
  JSON_EXTRACT_SCALAR(web_purchase_roas, '$.value') AS web_purchase_roas_value,
FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
  UNNEST(JSON_EXTRACT_ARRAY(mobile_app_purchase_roas)) AS roas,
  UNNEST(JSON_EXTRACT_ARRAY(purchase_roas)) AS p_roas,
  UNNEST(JSON_EXTRACT_ARRAY(website_purchase_roas)) AS web_purchase_roas