CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_action_values`
AS
SELECT 
  ad_id,
  adset_id,
  campaign_id,
  date_start,

  -- action_values,
  sum(safe_cast(JSON_EXTRACT_SCALAR(act_val,'$.1d_click') as int)) AS action_values_1d_click,
  sum(safe_cast(JSON_EXTRACT_SCALAR(act_val,'$.1d_view') as int)) AS action_values_1d_view,
  sum(safe_cast(JSON_EXTRACT_SCALAR(act_val,'$.28d_click') as int)) AS action_values_28d__click,
  sum(safe_cast(JSON_EXTRACT_SCALAR(act_val,'$.28d_view') as int)) AS action_values_28d_view,
  sum(safe_cast(JSON_EXTRACT_SCALAR(act_val,'$.7d_click') as int)) AS action_values_7d_click,
  sum(safe_cast(JSON_EXTRACT_SCALAR(act_val,'$.7d_view') as int)) AS action_values_7d_view,
  JSON_EXTRACT_SCALAR(act_val, '$.action_type') AS action_values_action_type,
  sum(safe_cast(JSON_EXTRACT_SCALAR(act_val, '$.value') as int)) AS action_values_value,

  FROM
(
select
*,
row_number() over(partition by ad_id, date_start,JSON_EXTRACT_SCALAR(act_val, '$.action_type'),_airbyte_extracted_at order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device,
UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_val
)
group by ad_id,date_start,action_values_action_type,adset_id,campaign_id
order by ad_id,date_start,action_values_action_type