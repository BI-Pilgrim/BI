merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_action_values` as target
using
(
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
where date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
)
group by ad_id,date_start,action_values_action_type,adset_id,campaign_id
order by ad_id,date_start,action_values_action_type
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.action_values_action_type = source.action_values_action_type
when matched and target.date_start < source.date_start
then update set
  target.ad_id = source.ad_id,
  target.adset_id = source.adset_id,
  target.campaign_id = source.campaign_id,
  target.date_start = source.date_start,
  target.action_values_1d_click = source.action_values_1d_click,
  target.action_values_1d_view = source.action_values_1d_view,
  target.action_values_28d__click = source.action_values_28d__click,
  target.action_values_28d_view = source.action_values_28d_view,
  target.action_values_7d_click = source.action_values_7d_click,
  target.action_values_7d_view = source.action_values_7d_view,
  target.action_values_action_type = source.action_values_action_type,
  target.action_values_value = source.action_values_value
when not matched
then insert
(
  ad_id,
  adset_id,
  campaign_id,
  date_start,
  action_values_1d_click,
  action_values_1d_view,
  action_values_28d__click,
  action_values_28d_view,
  action_values_7d_click,
  action_values_7d_view,
  action_values_action_type,
  action_values_value
)
values
(
  source.ad_id,
  source.adset_id,
  source.campaign_id,
  source.date_start,
  source.action_values_1d_click,
  source.action_values_1d_view,
  source.action_values_28d__click,
  source.action_values_28d_view,
  source.action_values_7d_click,
  source.action_values_7d_view,
  source.action_values_action_type,
  source.action_values_value
)