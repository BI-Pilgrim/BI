merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_action_values` as target
using
(
SELECT
  ad_id,
  date_start,
  gender,
  age,
  adset_id,
  campaign_id,
  account_id,

  -- action_values,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.1d_click') as float64)) AS action_values_1d_click,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.1d_view') as float64)) AS action_values_1d_view,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.28d_click') as float64)) AS action_values_28d_click,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.28d_view') as float64)) AS action_values_28d_view,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.7d_click') as float64)) AS action_values_7d_click,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.7d_view') as float64)) AS action_values_7d_view,
  JSON_EXTRACT_SCALAR(act_values, '$.action_type') AS action_values_action_type,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.value') as float64)) AS action_values_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,gender,age,JSON_EXTRACT_SCALAR(act_values, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender,
UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_values
where date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
)
group by all
order by action_values_action_type
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.gender = source.gender
and target.age = source.age
and target.action_values_action_type = source.action_values_action_type
when matched and target.date_start < source.date_start
then update set
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.gender = source.gender,
target.age = source.age,
target.adset_id = source.adset_id,
target.campaign_id = source.campaign_id,
target.account_id = source.account_id,
target.action_values_1d_click = source.action_values_1d_click,
target.action_values_1d_view = source.action_values_1d_view,
target.action_values_28d_click = source.action_values_28d_click,
target.action_values_28d_view = source.action_values_28d_view,
target.action_values_7d_click = source.action_values_7d_click,
target.action_values_7d_view = source.action_values_7d_view,
target.action_values_action_type = source.action_values_action_type,
target.action_values_value = source.action_values_value
when not matched
then insert
(
  ad_id,
  date_start,
  gender,
  age,
  adset_id,
  campaign_id,
  action_values_1d_click,
  action_values_1d_view,
  action_values_28d_click,
  action_values_28d_view,
  action_values_7d_click,
  action_values_7d_view,
  action_values_action_type,
  action_values_value
)
values
(
  source.ad_id,
  source.date_start,
  source.gender,
  source.age,
  source.adset_id,
  source.campaign_id,
  source.action_values_1d_click,
  source.action_values_1d_view,
  source.action_values_28d_click,
  source.action_values_28d_view,
  source.action_values_7d_click,
  source.action_values_7d_view,
  source.action_values_action_type,
  source.action_values_value
)