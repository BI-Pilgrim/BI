merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_conversion_device_actions` as target
using
(
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
  where date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
)
group by ad_id,date_start,actions_action_type,adset_id,campaign_id
order by ad_id,date_start,actions_action_type
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.actions_action_type = source.actions_action_type
when matched and target.date_start < source.date_start
then update set
target.ad_id = source.ad_id,
target.adset_id = source.adset_id,
target.campaign_id = source.campaign_id,
target.date_start = source.date_start,
target.actions_1d_click = source.actions_1d_click,
target.actions_1d_view = source.actions_1d_view,
target.actions_28d_click = source.actions_28d_click,
target.actions_7d_click = source.actions_7d_click,
target.actions_7d_view = source.actions_7d_view,
target.actions_action_type = source.actions_action_type,
target.actions_value = source.actions_value
when not matched
then insert
(
  ad_id,
  adset_id,
  campaign_id,
  date_start,
  actions_1d_click,
  actions_1d_view,
  actions_28d_click,
  actions_7d_click,
  actions_7d_view,
  actions_action_type,
  actions_value
)
values
(
source.ad_id,
source.adset_id,
source.campaign_id,
source.date_start,
source.actions_1d_click,
source.actions_1d_view,
source.actions_28d_click,
source.actions_7d_click,
source.actions_7d_view,
source.actions_action_type,
source.actions_value
)