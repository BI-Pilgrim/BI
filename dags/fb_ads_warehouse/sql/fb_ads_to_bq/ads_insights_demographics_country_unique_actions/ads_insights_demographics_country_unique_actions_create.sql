CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_demographics_country_unique_actions`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  country,
  adset_id,
  date_start,
  campaign_id,
  

  -- cost_per_action_type,
  JSON_EXTRACT_SCALAR(unique_acts, '$.1d_click') AS unique_actions_1d_click,
  JSON_EXTRACT_SCALAR(unique_acts, '$.1d_view') AS unique_actions_1d_view,
  JSON_EXTRACT_SCALAR(unique_acts, '$.28d_click') AS unique_actions_28d_click,
  JSON_EXTRACT_SCALAR(unique_acts, '$.28d_views') AS unique_actions_28d_views,  
  JSON_EXTRACT_SCALAR(unique_acts, '$.7d_click') AS unique_actions_7d_click,
  JSON_EXTRACT_SCALAR(unique_acts, '$.7d_view') AS unique_actions_7d_view,
  JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') AS unique_actions_action_type,
  JSON_EXTRACT_SCALAR(unique_acts, '$.value') AS unique_actions_value,



FROM
(
select
*,
row_number() over(partition by ad_id,date_start,country,JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') order by _airbyte_extracted_at) as rn
FROM shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_demographics_country,
unnest(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_acts
)
where rn = 1