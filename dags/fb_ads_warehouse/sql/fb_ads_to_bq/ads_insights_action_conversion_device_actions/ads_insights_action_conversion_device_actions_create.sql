CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_actions`
AS
SELECT 
  ad_id,
  adset_id,
  campaign_id,
  date_start,


  -- actions,
  sum(safe_cast(JSON_EXTRACT_SCALAR(acts, '$.1d_click') as int)) AS actions_1d_click,
  sum(safe_cast(JSON_EXTRACT_SCALAR(acts, '$.1d_view') as int)) AS actions_1d_view,
  sum(safe_cast(JSON_EXTRACT_SCALAR(acts, '$.28d_click') as int)) AS actions_28d_click,
  sum(safe_cast(JSON_EXTRACT_SCALAR(acts, '$.28d_view') as int)) AS actions_28d_view,
  sum(safe_cast(JSON_EXTRACT_SCALAR(acts, '$.7d_click') as int)) AS actions_7d_click,
  sum(safe_cast(JSON_EXTRACT_SCALAR(acts, '$.7d_view') as int)) AS actions_7d_view,
  JSON_EXTRACT_SCALAR(acts, '$.action_type') AS actions_action_type,
  sum(safe_cast(JSON_EXTRACT_SCALAR(acts, '$.value') as int)) AS actions_value,
  FROM
(
  select
  *,
  row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(acts, '$.action_type') order by _airbyte_extracted_at desc) as rn
  from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device,
  UNNEST(JSON_EXTRACT_ARRAY(actions)) AS acts
)
group by ad_id,date_start,actions_action_type,adset_id,campaign_id
order by ad_id,date_start,actions_action_type

