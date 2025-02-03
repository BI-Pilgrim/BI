merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_cost_per_unique_action_type` as target
using
(
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
where date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
)
group by ad_id,date_start,cost_per_unique_act_type_action_type,adset_id,campaign_id
order by ad_id,date_start,cost_per_unique_act_type_action_type
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.cost_per_unique_act_type_action_type = source.cost_per_unique_act_type_action_type
when matched and target.date_start < source.date_start
then update set
  target.ad_id = source.ad_id,
  target.adset_id = source.adset_id,
  target.campaign_id = source.campaign_id,
  target.date_start = source.date_start,
  target.cost_per_unique_act_type_1d_click = source.cost_per_unique_act_type_1d_click,
  target.cost_per_unique_act_type_1d_view = source.cost_per_unique_act_type_1d_view,
  target.cost_per_unique_act_type_7d_click = source.cost_per_unique_act_type_7d_click,
  target.cost_per_unique_act_type_7d_view = source.cost_per_unique_act_type_7d_view,
  target.cost_per_unique_act_type_28d_click = source.cost_per_unique_act_type_28d_click,
  target.cost_per_unique_act_type_28d_view = source.cost_per_unique_act_type_28d_view,
  target.cost_per_unique_act_type_action_type = source.cost_per_unique_act_type_action_type,
  target.cost_per_unique_act_type_value = source.cost_per_unique_act_type_value
when not matched
then insert
(
  ad_id,
  adset_id,
  campaign_id,
  date_start,
  cost_per_unique_act_type_1d_click,
  cost_per_unique_act_type_1d_view,
  cost_per_unique_act_type_7d_click,
  cost_per_unique_act_type_7d_view,
  cost_per_unique_act_type_28d_click,
  cost_per_unique_act_type_28d_view,
  cost_per_unique_act_type_action_type,
  cost_per_unique_act_type_value
)
values
(
source.ad_id,
source.adset_id,
source.campaign_id,
source.date_start,
source.cost_per_unique_act_type_1d_click,
source.cost_per_unique_act_type_1d_view,
source.cost_per_unique_act_type_7d_click,
source.cost_per_unique_act_type_7d_view,
source.cost_per_unique_act_type_28d_click,
source.cost_per_unique_act_type_28d_view,
source.cost_per_unique_act_type_action_type,
source.cost_per_unique_act_type_value
)