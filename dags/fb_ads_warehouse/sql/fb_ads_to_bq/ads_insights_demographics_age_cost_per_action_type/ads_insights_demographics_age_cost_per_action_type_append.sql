merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_age_cost_per_action_type` as target
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
  JSON_EXTRACT_SCALAR(cpat, '$.1d_click') AS cost_per_action_type_1d_click,
  JSON_EXTRACT_SCALAR(cpat, '$.1d_view') AS cost_per_action_type_1d_view,
  JSON_EXTRACT_SCALAR(cpat, '$.28d_click') AS cost_per_action_type_28d_click,
  JSON_EXTRACT_SCALAR(cpat, '$.28d_views') AS cost_per_action_type_28d_views,  
  JSON_EXTRACT_SCALAR(cpat, '$.7d_click') AS cost_per_action_type_7d_click,
  JSON_EXTRACT_SCALAR(cpat, '$.7d_view') AS cost_per_action_type_7d_view,
  JSON_EXTRACT_SCALAR(cpat, '$.action_type') AS cost_per_action_type_action_type,
  JSON_EXTRACT_SCALAR(cpat, '$.value') AS cost_per_action_type_value,


FROM
(
select
*,
row_number() over(partition by ad_id,date_start,age,JSON_EXTRACT_SCALAR(cpat, '$.action_type') order by _airbyte_extracted_at) as rn
FROM shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_age,
unnest(JSON_EXTRACT_ARRAY(cost_per_action_type)) AS cpat
)
where rn = 1 and date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.age = source.age
and target.cost_per_action_type_action_type = source.cost_per_action_type_action_type
when matched and target.date_start < source.date_start
then update set
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.age = source.age,
target.adset_id = source.adset_id,
target.campaign_id = source.campaign_id,
target.cost_per_action_type_1d_click = source.cost_per_action_type_1d_click,
target.cost_per_action_type_1d_view = source.cost_per_action_type_1d_view,
target.cost_per_action_type_28d_click = source.cost_per_action_type_28d_click,
target.cost_per_action_type_28d_views = source.cost_per_action_type_28d_views,
target.cost_per_action_type_7d_click = source.cost_per_action_type_7d_click,
target.cost_per_action_type_7d_view = source.cost_per_action_type_7d_view,
target.cost_per_action_type_action_type = source.cost_per_action_type_action_type,
target.cost_per_action_type_value = source.cost_per_action_type_value
when not matched
then insert
(
  _airbyte_extracted_at,
  ad_id,
  date_start,
  age,
  adset_id,
  campaign_id,
  cost_per_action_type_1d_click,
  cost_per_action_type_1d_view,
  cost_per_action_type_28d_click,
  cost_per_action_type_28d_views,  
  cost_per_action_type_7d_click,
  cost_per_action_type_7d_view,
  cost_per_action_type_action_type,
  cost_per_action_type_value
)
values
(
source._airbyte_extracted_at,
source.ad_id,
source.date_start,
source.age,
source.adset_id,
source.campaign_id,
source.cost_per_action_type_1d_click,
source.cost_per_action_type_1d_view,
source.cost_per_action_type_28d_click,
source.cost_per_action_type_28d_views,
source.cost_per_action_type_7d_click,
source.cost_per_action_type_7d_view,
source.cost_per_action_type_action_type,
source.cost_per_action_type_value
)