merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_actions` as target
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


  -- actions,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.1d_click') as float64)) AS actions_1d_click,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.1d_view') as float64)) AS actions_1d_view,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.28d_click') as float64)) AS actions_28d_click,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.28d_view') as float64)) AS actions_28d_view,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.7d_click') as float64)) AS actions_7d_click,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.7d_view') as float64)) AS actions_7d_view,
  JSON_EXTRACT_SCALAR(acts, '$.action_type') AS actions_action_type,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.value') as float64)) AS actions_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,gender,age,JSON_EXTRACT_SCALAR(acts, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender,
UNNEST(JSON_EXTRACT_ARRAY(actions)) AS acts
where date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
)
group by all
order by actions_action_type
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.gender = source.gender
and target.age = source.age
and target.actions_action_type = source.actions_action_type
when matched and target.date_start < source.date_start
then update set
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.gender = source.gender,
target.age = source.age,
target.adset_id = source.adset_id,
target.campaign_id = source.campaign_id,
target.account_id = source.account_id,
target.actions_1d_click = source.actions_1d_click,
target.actions_1d_view = source.actions_1d_view,
target.actions_28d_click = source.actions_28d_click,
target.actions_28d_view = source.actions_28d_view,
target.actions_7d_click = source.actions_7d_click,
target.actions_7d_view = source.actions_7d_view,
target.actions_action_type = source.actions_action_type,
target.actions_value = source.actions_value
when not matched
then insert
(
  ad_id,
  date_start,
  gender,
  age,
  adset_id,
  campaign_id,
  account_id,
  actions_1d_click,
  actions_1d_view,
  actions_28d_click,
  actions_28d_view,
  actions_7d_click,
  actions_7d_view,
  actions_action_type,
  actions_value
)
values
(
source.ad_id,
source.date_start,
source.gender,
source.age,
source.adset_id,
source.campaign_id,
source.account_id,
source.actions_1d_click,
source.actions_1d_view,
source.actions_28d_click,
source.actions_28d_view,
source.actions_7d_click,
source.actions_7d_view,
source.actions_action_type,
source.actions_value
)