merge into `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_unique_actions` as target
using
(
SELECT
  campaign_id,
  adset_id,
  ad_id,
  date_start,
  country,
  account_id,


  -- unique_actions,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.1d_click') as float64)) AS unique_actions_1d_click,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.1d_view') as float64)) AS unique_actions_1d_view,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.28d_click') as float64)) AS unique_actions_28d_click,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.28d_click') as float64)) AS unique_actions_28d_view,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.7d_click') as float64)) AS unique_actions_7d_click,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.7d_view') as float64)) AS unique_actions_7d_view,
  JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') AS unique_actions_action_type,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.value') as float64)) AS unique_actions_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,country,JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country,
UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_acts
where date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), INTERVAL 10 day)
)
group by all
order by ad_id,date_start,country,unique_actions_action_type
) as source
on target.ad_id = source.ad_id
and target.date_start = source.date_start
and target.country = source.country
and target.unique_actions_action_type = source.unique_actions_action_type
when matched and target.date_start < source.date_start
then update set
  target.campaign_id = source.campaign_id,
  target.adset_id = source.adset_id,
  target.ad_id = source.ad_id,
  target.date_start = source.date_start,
  target.country = source.country,
  target.account_id = source.account_id,
  target.unique_actions_1d_click = source.unique_actions_1d_click,
  target.unique_actions_1d_view = source.unique_actions_1d_view,
  target.unique_actions_28d_click = source.unique_actions_28d_click,
  target.unique_actions_28d_view = source.unique_actions_28d_view,
  target.unique_actions_7d_click = source.unique_actions_7d_click,
  target.unique_actions_7d_view = source.unique_actions_7d_view,
  target.unique_actions_action_type = source.unique_actions_action_type,
  target.unique_actions_value = source.unique_actions_value
when not matched
then insert
(
  campaign_id,
  adset_id,
  ad_id,
  date_start,
  country,
  account_id,
  unique_actions_1d_click,
  unique_actions_1d_view,
  unique_actions_28d_click,
  unique_actions_28d_view,
  unique_actions_7d_click,
  unique_actions_7d_view,
  unique_actions_action_type,
  unique_actions_value
)
values
(
  source.campaign_id,
  source.adset_id,
  source.ad_id,
  source.date_start,
  source.country,
  source.account_id,
  source.unique_actions_1d_click,
  source.unique_actions_1d_view,
  source.unique_actions_28d_click,
  source.unique_actions_28d_view,
  source.unique_actions_7d_click,
  source.unique_actions_7d_view,
  source.unique_actions_action_type,
  source.unique_actions_value
)