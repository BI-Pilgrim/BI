merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_device_actions` as target
using
(
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
where rn = 1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.device_platform = source.device_platform
and target.actions_action_type = source.actions_action_type
when matched and target.date_start < source.date_start
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.device_platform = source.device_platform,
target.adset_id = source.adset_id,
target.account_id = source.account_id,
target.campaign_id = source.campaign_id,
target.actions_1d_click = source.actions_1d_click,
target.actions_1d_view = source.actions_1d_view,
target.actions_28d_click = source.actions_28d_click,
target.actions_28d_views = source.actions_28d_views,
target.actions_7d_click = source.actions_7d_click,
target.actions_7d_view = source.actions_7d_view,
target.actions_action_type = source.actions_action_type,
target.actions_value = source.actions_value
when not matched
then insert
(
  _airbyte_extracted_at,
  ad_id,
  date_start,
  device_platform,
  adset_id,
  account_id,
  campaign_id,
  actions_1d_click,
  actions_1d_view,
  actions_28d_click,
  actions_28d_views,  
  actions_7d_click,
  actions_7d_view,
  actions_action_type,
  actions_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.date_start,
source.device_platform,
source.adset_id,
source.account_id,
source.campaign_id,
source.actions_1d_click,
source.actions_1d_view,
source.actions_28d_click,
source.actions_28d_views,
source.actions_7d_click,
source.actions_7d_view,
source.actions_action_type,
source.actions_value
)