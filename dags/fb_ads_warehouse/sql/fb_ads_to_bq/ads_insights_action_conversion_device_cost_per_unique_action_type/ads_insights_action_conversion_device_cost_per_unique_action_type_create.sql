CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_cost_per_unique_action_type`
AS
SELECT
  ad_id,
  adset_id,
  campaign_id,
  date_start,


  -- cost_per_unique_action_type,
  sum(cast(JSON_EXTRACT_SCALAR(cpuat, '$.1d_click') as float64)) AS cost_per_unique_act_type_1d_click,
  sum(cast(JSON_EXTRACT_SCALAR(cpuat, '$.1d_view') as float64)) AS cost_per_unique_act_type_1d_view,
  sum(cast(JSON_EXTRACT_SCALAR(cpuat, '$.7d_click') as float64)) AS cost_per_unique_act_type_7d_click,
  sum(cast(JSON_EXTRACT_SCALAR(cpuat, '$.7d_view') as float64)) AS cost_per_unique_act_type_7d_view,
  sum(cast(JSON_EXTRACT_SCALAR(cpuat, '$.28d_click') as float64)) AS cost_per_unique_act_type_28d_click,
  sum(cast(JSON_EXTRACT_SCALAR(cpuat, '$.28d_view') as float64)) AS cost_per_unique_act_type_28d_view,
  JSON_EXTRACT_SCALAR(cpuat, '$.action_type') AS cost_per_unique_act_type_action_type,
  sum(cast(JSON_EXTRACT_SCALAR(cpuat, '$.value') as float64)) AS cost_per_unique_act_type_value,

FROM
(
select
*,
row_number() over(partition by ad_id, date_start,JSON_EXTRACT_SCALAR(cpuat, '$.action_type') order by _airbyte_extracted_at desc) as rn,
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_conversion_device,
UNNEST(JSON_EXTRACT_ARRAY(cost_per_unique_action_type)) AS cpuat
)
group by   ad_id,adset_id,campaign_id,date_start,cost_per_unique_act_type_action_type
order by ad_id, date_start, cost_per_unique_act_type_action_type
