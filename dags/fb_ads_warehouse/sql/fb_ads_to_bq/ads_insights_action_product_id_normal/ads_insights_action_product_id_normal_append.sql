MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_normal` AS TARGET
USING
(
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
(
  SELECT
  *,
  ROW_NUMBER() OVER(PARTITION BY ad_id ORDER BY date_stop) AS row_nuM
  FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id`
  WHERE DATE(date_stop) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_nuM = 1 
) AS SOURCE
ON SOURCE.ad_id = TARGET.ad_id
AND SOURCE.adset_id = TARGET.adset_id
AND SOURCE.campaign_id = TARGET.campaign_id
AND SOURCE.account_id = TARGET.account_id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
THEN UPDATE SET
TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
TARGET.cpc = SOURCE.cpc,
TARGET.cpm = SOURCE.cpm,
TARGET.ctr = SOURCE.ctr,
TARGET.ad_id = SOURCE.ad_id,
TARGET.spend = SOURCE.spend,
TARGET.clicks = SOURCE.clicks,
TARGET.ad_name = SOURCE.ad_name,
TARGET.adset_id = SOURCE.adset_id,
TARGET.date_stop = SOURCE.date_stop,
TARGET.objective = SOURCE.objective,
TARGET.account_id = SOURCE.account_id,
TARGET.adset_name = SOURCE.adset_name,
TARGET.date_start = SOURCE.date_start,
TARGET.product_id = SOURCE.product_id,
TARGET.buying_type = SOURCE.buying_type,
TARGET.campaign_id = SOURCE.campaign_id,
TARGET.impressions = SOURCE.impressions,
TARGET.account_name = SOURCE.account_name,
TARGET.created_time = SOURCE.created_time,
TARGET.updated_time = SOURCE.updated_time,
TARGET.campaign_name = SOURCE.campaign_name,
TARGET.account_currency = SOURCE.account_currency,
TARGET.optimization_goal = SOURCE.optimization_goal,
TARGET.inline_link_clicks = SOURCE.inline_link_clicks,
TARGET.full_view_impressions = SOURCE.full_view_impressions,
TARGET.inline_link_click_ctr = SOURCE.inline_link_click_ctr,
TARGET.inline_post_engagement = SOURCE.inline_post_engagement,
TARGET.cost_per_inline_link_click = SOURCE.cost_per_inline_link_click,
TARGET.cost_per_inline_post_engagement = SOURCE.cost_per_inline_post_engagement,
TARGET.website_ctr_action_type = SOURCE.website_ctr_action_type,
TARGET.website_ctr_value = SOURCE.website_ctr_value,
TARGET.outbound_clicks_action_type = SOURCE.outbound_clicks_action_type,
TARGET.outbound_clicks_value = SOURCE.outbound_clicks_value,
TARGET.outbound_clicks_ctr_action_type = SOURCE.outbound_clicks_ctr_action_type,
TARGET.outbound_clicks_ctr_value = SOURCE.outbound_clicks_ctr_value,
TARGET.cost_per_outbound_click_action_type = SOURCE.cost_per_outbound_click_action_type,
TARGET.cost_per_outbound_click_value = SOURCE.cost_per_outbound_click_value,
TARGET.mobile_app_purchase_roas_action_type = SOURCE.mobile_app_purchase_roas_action_type,
TARGET.mobile_app_purchase_roas_value = SOURCE.mobile_app_purchase_roas_value
WHEN NOT MATCHED
THEN INSERT
(
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
  website_ctr_action_type,
  website_ctr_value,
  outbound_clicks_action_type,  
  outbound_clicks_value,
  outbound_clicks_ctr_action_type,  
  outbound_clicks_ctr_value,
  cost_per_outbound_click_action_type,  
  cost_per_outbound_click_value,
  mobile_app_purchase_roas_action_type,  
  mobile_app_purchase_roas_value
)
VALUES
(
SOURCE._airbyte_extracted_at,
SOURCE.cpc,
SOURCE.cpm,
SOURCE.ctr,
SOURCE.ad_id,
SOURCE.spend,
SOURCE.clicks,
SOURCE.ad_name,
SOURCE.adset_id,
SOURCE.date_stop,
SOURCE.objective,
SOURCE.account_id,
SOURCE.adset_name,
SOURCE.date_start,
SOURCE.product_id,
SOURCE.buying_type,
SOURCE.campaign_id,
SOURCE.impressions,
SOURCE.account_name,
SOURCE.created_time,
SOURCE.updated_time,
SOURCE.campaign_name,
SOURCE.account_currency,
SOURCE.optimization_goal,
SOURCE.inline_link_clicks,
SOURCE.full_view_impressions,
SOURCE.inline_link_click_ctr,
SOURCE.inline_post_engagement,
SOURCE.cost_per_inline_link_click,
SOURCE.cost_per_inline_post_engagement,
SOURCE.website_ctr_action_type,
SOURCE.website_ctr_value,
SOURCE.outbound_clicks_action_type,
SOURCE.outbound_clicks_value,
SOURCE.outbound_clicks_ctr_action_type,
SOURCE.outbound_clicks_ctr_value,
SOURCE.cost_per_outbound_click_action_type,
SOURCE.cost_per_outbound_click_value,
SOURCE.mobile_app_purchase_roas_action_type,
SOURCE.mobile_app_purchase_roas_value
)