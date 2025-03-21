CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_cost_per_action_type`
AS
SELECT
  _airbyte_extracted_at,
  ad_id,
  date_start,
  rn,
  product_id
  adset_id,
  account_id,
  campaign_id,


  -- cost_per_action_type,
  JSON_EXTRACT_SCALAR(cpat, '$.1d_click') AS cost_per_action_type_1d_click,
  JSON_EXTRACT_SCALAR(cpat, '$.1d_view') AS cost_per_action_type_1d_view,
  JSON_EXTRACT_SCALAR(cpat, '$.28d_click') AS cost_per_action_type_28d_click,
  JSON_EXTRACT_SCALAR(cpat, '$.28d_views') AS cost_per_action_type_28d_views,
  JSON_EXTRACT_SCALAR(cpat, '$.7d_click') AS cost_per_action_type_7d_click,
  JSON_EXTRACT_SCALAR(cpat, '$.7d_view') AS cost_per_action_type_7d_view,
  JSON_EXTRACT_SCALAR(cpat, '$.action_type') AS cost_per_action_type_action_type,
  JSON_EXTRACT_SCALAR(cpat, '$.value') AS cost_per_action_type_value,
FROM
(
select
*,
row_number() over(partition by ad_id, date_start,product_id,JSON_EXTRACT_SCALAR(cpat, '$.action_type') order by _airbyte_extracted_at desc ) as rn
from shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id,
UNNEST(JSON_EXTRACT_ARRAY(cost_per_action_type)) AS cpat
)
where rn = 1
order by ad_id, date_start,product_id,cost_per_action_type_action_type,rn
