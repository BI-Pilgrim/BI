CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_video_sound_actions`
AS
SELECT
  ad_id,
  date_start,
  adset_id,
  campaign_id,


  -- actions,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.1d_click') as float64)) AS actions_1d_click,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.1d_view') as float64)) AS actions_1d_view,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.28d_click') as float64)) AS actions_28d_click,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.28d_view') as float64)) AS actions_28d_view,  
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.7d_click') as float64)) AS actions_7d_click,
  sum(cast(JSON_EXTRACT_SCALAR(acts, '$.7d_view') as float64)) AS actions_7d_view,
  JSON_EXTRACT_SCALAR(acts, '$.action_type') AS actions_action_type,
  JSON_EXTRACT_SCALAR(acts, '$.value') AS actions_value,
FROM
(
select
*,
row_number() over(partition by ad_id, date_start,JSON_EXTRACT_SCALAR(acts, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_video_sound,
UNNEST(JSON_EXTRACT_ARRAY(actions)) AS acts
)
group by all