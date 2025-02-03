CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_actions`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  date_start,
  device_platform,
  adset_id,
  account_id,
  campaign_id,


  -- actions,
  JSON_EXTRACT_SCALAR(acts, '$.1d_click') AS actions_1d_click,
  JSON_EXTRACT_SCALAR(acts, '$.1d_view') AS actions_1d_view,
  JSON_EXTRACT_SCALAR(acts, '$.28d_click') AS actions_28d_click,
  JSON_EXTRACT_SCALAR(acts, '$.28d_views') AS actions_28d_views,  
  JSON_EXTRACT_SCALAR(acts, '$.7d_click') AS actions_7d_click,
  JSON_EXTRACT_SCALAR(acts, '$.7d_view') AS actions_7d_view,
  JSON_EXTRACT_SCALAR(acts, '$.action_type') AS actions_action_type,
  JSON_EXTRACT_SCALAR(acts, '$.value') AS actions_value,


FROM
(
select
*,
row_number() over(partition by ad_id,date_start,device_platform,JSON_EXTRACT_SCALAR(acts, '$.action_type') order by _airbyte_extracted_at) as rn
FROM shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device,
UNNEST(JSON_EXTRACT_ARRAY(actions)) AS acts
)
where rn = 1