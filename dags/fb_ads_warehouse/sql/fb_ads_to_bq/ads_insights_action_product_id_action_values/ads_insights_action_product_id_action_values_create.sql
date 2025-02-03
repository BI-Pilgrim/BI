CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_action_values`
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  date_start,
  adset_id,
  account_id,
  campaign_id,
  product_id,


  -- action_values,
  JSON_EXTRACT_SCALAR(act_values, '$.1d_click') AS act_values_1d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.1d_view') AS act_values_1d_view,
  JSON_EXTRACT_SCALAR(act_values, '$.28d_click') AS act_values_28d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.28d_views') AS act_values_28d_views,  
  JSON_EXTRACT_SCALAR(act_values, '$.7d_click') AS act_values_7d_click,
  JSON_EXTRACT_SCALAR(act_values, '$.7d_view') AS act_values_7d_view,
  JSON_EXTRACT_SCALAR(act_values, '$.action_type') AS act_values_action_type,
  JSON_EXTRACT_SCALAR(act_values, '$.value') AS act_values_value,
FROM
(
select
*,
row_number() over(partition by ad_id,date_start,product_id,JSON_EXTRACT_SCALAR(act_values, '$.action_type') order by _airbyte_extracted_at desc) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id,
UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_values
)
where rn = 1