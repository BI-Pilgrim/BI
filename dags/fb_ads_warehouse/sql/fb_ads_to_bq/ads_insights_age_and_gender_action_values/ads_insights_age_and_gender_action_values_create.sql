CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_action_values`
AS
SELECT
--   _airbyte_extracted_at,
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
)
group by all
order by action_values_action_type