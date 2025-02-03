CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_unique_actions`
AS
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
)
group by all
-- where ad_id = '120200253049710111' and date_start = '2023-10-31' and country = 'IN'
order by ad_id,date_start,country,unique_actions_action_type
