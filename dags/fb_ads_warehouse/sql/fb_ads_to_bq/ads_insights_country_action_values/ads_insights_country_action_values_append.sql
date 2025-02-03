merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_action_values` as target
using
(
SELECT
  campaign_id,
  adset_id,
  ad_id,
  date_start,
  country,
  account_id,


  -- action_values,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.1d_click') as float64)) AS action_values_1d_click,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.1d_view') as float64)) AS action_values_1d_view,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.28d_click') as float64)) AS action_values_28d_click,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.28d_click') as float64)) AS action_values_28d_view,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.7d_click') as float64)) AS action_values_7d_click,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.7d_view') as float64)) AS action_values_7d_view,
  JSON_EXTRACT_SCALAR(act_values, '$.action_type') AS action_values_action_type,
  sum(cast(JSON_EXTRACT_SCALAR(act_values, '$.value') as float64)) AS action_values_value,
FROM
(
select
*,
row_number() over(partition by ad_id, date_start,country,JSON_EXTRACT_SCALAR(act_values, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country,
UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_values
where date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
)
group by all
order by ad_id, date_start,country,action_values_action_type 
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.country = source.country
and target.action_values_action_type = source.action_values_action_type
when matched and target.date_start < source.date_start
then update set
target.campaign_id = source.campaign_id,
target.adset_id = source.adset_id,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.country = source.country,
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
  campaign_id,
  adset_id,
  ad_id,
  date_start,
  country,
  account_id,
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
  source.campaign_id,
  source.adset_id,
  source.ad_id,
  source.date_start,
  source.country,
  source.account_id,
  source.action_values_1d_click,
  source.action_values_1d_view,
  source.action_values_28d_click,
  source.action_values_28d_view,
  source.action_values_7d_click,
  source.action_values_7d_view,
  source.action_values_action_type,
  source.action_values_value
)