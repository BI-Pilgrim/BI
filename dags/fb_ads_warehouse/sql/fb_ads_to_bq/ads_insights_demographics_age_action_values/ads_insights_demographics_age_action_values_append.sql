merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_age_action_values` as target
using
(
SELECT
  _airbyte_extracted_at,
  ad_id,
  date_start,
  age,
  adset_id,
  campaign_id,



  -- cost_per_action_type,
  JSON_EXTRACT_SCALAR(act_values, '$.1d_click') AS action_values_1d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.1d_view') AS action_values_1d_view,
  JSON_EXTRACT_SCALAR(act_values, '$.28d_click') AS action_values_28d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.28d_views') AS action_values_28d_views,  
  JSON_EXTRACT_SCALAR(act_values, '$.7d_click') AS action_values_7d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.7d_view') AS action_values_7d_view,
  JSON_EXTRACT_SCALAR(act_values, '$.action_type') AS action_values_action_type,
  JSON_EXTRACT_SCALAR(act_values, '$.value') AS action_values_value,


FROM
(
select
*,
row_number() over(partition by ad_id,date_start,age,JSON_EXTRACT_SCALAR(act_values, '$.action_type') order by _airbyte_extracted_at) as rn
FROM shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age,
unnest(JSON_EXTRACT_ARRAY(action_values)) AS act_values
)
where rn = 1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.age = source.age
and target.action_values_action_type = source.action_values_action_type
when matched and target.date_start < source.date_start
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.age = source.age,
target.adset_id = source.adset_id,
target.campaign_id = source.campaign_id,
target.action_values_1d_click = source.action_values_1d_click,
target.action_values_1d_view = source.action_values_1d_view,
target.action_values_28d_click = source.action_values_28d_click,
target.action_values_28d_views = source.action_values_28d_views,
target.action_values_7d_click = source.action_values_7d_click,
target.action_values_7d_view = source.action_values_7d_view,
target.action_values_action_type = source.action_values_action_type,
target.action_values_value = source.action_values_value
when not matched
then insert
(
  _airbyte_extracted_at,
  ad_id,
  date_start,
  age,
  adset_id,
  campaign_id,
  action_values_1d_click,
  action_values_1d_view,
  action_values_28d_click,
  action_values_28d_views,  
  action_values_7d_click,
  action_values_7d_view,
  action_values_action_type,
  action_values_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.date_start,
source.age,
source.adset_id,
source.campaign_id,
source.action_values_1d_click,
source.action_values_1d_view,
source.action_values_28d_click,
source.action_values_28d_views,
source.action_values_7d_click,
source.action_values_7d_view,
source.action_values_action_type,
source.action_values_value
)