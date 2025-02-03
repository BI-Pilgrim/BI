CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_unique_actions`
as
SELECT
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  campaign_id,
  date_start,
  device_platform,

  -- unique_actions,
  JSON_EXTRACT_SCALAR(unique_acts, '$.1d_click') AS unique_actions_1d_click,
  JSON_EXTRACT_SCALAR(unique_acts, '$.1d_view') AS unique_actions_1d_view,
  JSON_EXTRACT_SCALAR(unique_acts, '$.28d_click') AS unique_actions_28d_click,
  JSON_EXTRACT_SCALAR(unique_acts, '$.28d_view') AS unique_actions_28d_view,
  JSON_EXTRACT_SCALAR(unique_acts, '$.7d_click') AS unique_actions_7d_click,
  JSON_EXTRACT_SCALAR(unique_acts, '$.7d_view') AS unique_actions_7d_view,
  JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') AS unique_actions_action_type,
  JSON_EXTRACT_SCALAR(unique_acts, '$.value') AS unique_actions_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,device_platform,JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device,
UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_acts
)
where rn = 1
order by ad_id,date_start,device_platform,rn