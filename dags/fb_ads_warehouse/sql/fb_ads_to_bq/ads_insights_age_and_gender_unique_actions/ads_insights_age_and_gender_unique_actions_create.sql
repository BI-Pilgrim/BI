CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_age_and_gender_unique_actions`
AS
SELECT
  ad_id,
  date_start,
  age,
  gender,
  adset_id,
  campaign_id,
  account_id,


  -- unique_actions,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.1d_click') as float64)) AS unique_actions_1d_click,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.1d_view') as float64)) AS unique_actions_1d_view,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.28d_click') as float64)) AS unique_actions_28d_click,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.28d_view') as float64)) AS unique_actions_28d_view,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.7d_click') as float64)) AS unique_actions_7d_click,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.7d_view') as float64)) AS unique_actions_7d_view,
  JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') AS unique_actions_action_type,
  sum(cast(JSON_EXTRACT_SCALAR(unique_acts, '$.value') as float64)) AS unique_actions_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,gender,age,JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') order by _airbyte_extracted_at) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_age_and_gender,
UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_acts
where date(_airbyte_extracted_at) >= date_sub(current_date("Asia/Kolkata"), interval 10 day)
)
group by all
order by unique_actions_action_type