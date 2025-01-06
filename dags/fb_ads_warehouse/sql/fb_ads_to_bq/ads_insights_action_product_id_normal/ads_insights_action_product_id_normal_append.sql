CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_normal`
AS
SELECT
  _airbyte_extracted_at,
  cpc,
  cpm,
  ctr,
  ad_id,
  spend,
  clicks,
  ad_name,
  adset_id,
  date_stop,
  objective,
  account_id,
  adset_name,
  date_start,
  product_id,
  buying_type,
  campaign_id,
  impressions,
  account_name,
  created_time,
  updated_time,
  campaign_name,
  account_currency,
  optimization_goal,
  inline_link_clicks,
  full_view_impressions,
  inline_link_click_ctr,
  inline_post_engagement,
  cost_per_inline_link_click,
  cost_per_inline_post_engagement,


  -- website_ctr, --NORMAL
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_ctr, '$[0]'), '$.action_type') AS website_ctr_action_type,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(website_ctr, '$[0]'), '$.value') AS website_ctr_value,


  -- outbound_clicks,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks, '$[0]'), '$.action_type') AS outbound_clicks_action_type,  
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks, '$[0]'), '$.value') AS outbound_clicks_value,


  -- outbound_clicks_ctr,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks_ctr, '$[0]'), '$.action_type') AS outbound_clicks_ctr_action_type,  
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(outbound_clicks_ctr, '$[0]'), '$.value') AS outbound_clicks_ctr_value,


  -- cost_per_outbound_click,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_outbound_click, '$[0]'), '$.action_type') AS cost_per_outbound_click_action_type,  
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(cost_per_outbound_click, '$[0]'), '$.value') AS cost_per_outbound_click_value,


  -- mobile_app_purchase_roas,
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(mobile_app_purchase_roas, '$[0]'), '$.action_type') AS mobile_app_purchase_roas_action_type,  
  JSON_EXTRACT_SCALAR(JSON_EXTRACT(mobile_app_purchase_roas, '$[0]'), '$.value') AS mobile_app_purchase_roas_value,
FROM
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id