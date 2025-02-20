MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_cost_per_conv_value` AS TARGET
USING
(
select * except(rn)
from
(
SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  date_start,

  JSON_EXTRACT_SCALAR(cost_per_conv, '$.1d_click') AS cost_per_conv_1d_click,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.1d_view') AS cost_per_conv_1d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.28d_click') AS cost_per_conv_28d_click,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.28d_view') AS cost_per_conv_28d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.7d_click') AS cost_per_conv_7d_click,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.7d_view') AS cost_per_conv_7d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.action_type') AS cost_per_conv_action_type,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.value') AS cost_per_conv_value,

  row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(cost_per_conv, '$.action_type') order by _airbyte_extracted_at desc) as rn
FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`,
  UNNEST(JSON_EXTRACT_ARRAY(cost_per_conversion)) AS cost_per_conv
)
where rn = 1
) AS SOURCE
ON TARGET.ad_id = SOURCE.ad_id
and TARGET.date_start = SOURCE.date_start
and TARGET.cost_per_conv_action_type = SOURCE.cost_per_conv_action_type
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at 
THEN UPDATE SET
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.campaign_id = source.campaign_id,
target.adset_id = source.adset_id,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.cost_per_conv_1d_click = source.cost_per_conv_1d_click,  
target.cost_per_conv_1d_view = source.cost_per_conv_1d_view,  
target.cost_per_conv_28d_click = source.cost_per_conv_28d_click,  
target.cost_per_conv_28d_view = source.cost_per_conv_28d_view,  
target.cost_per_conv_7d_click = source.cost_per_conv_7d_click,  
target.cost_per_conv_7d_view = source.cost_per_conv_7d_view,  
target.cost_per_conv_action_type = source.cost_per_conv_action_type,  
target.cost_per_conv_value = source.cost_per_conv_value 
WHEN NOT MATCHED
THEN INSERT
(
_airbyte_extracted_at,
campaign_id,
adset_id,
ad_id,
date_start,
cost_per_conv_1d_click,
cost_per_conv_1d_view,
cost_per_conv_28d_click,
cost_per_conv_28d_view,
cost_per_conv_7d_click,
cost_per_conv_7d_view,
cost_per_conv_action_type,
cost_per_conv_value
)
VALUES
(
source._airbyte_extracted_at,
source.campaign_id,
source.adset_id,
source.ad_id,
source.date_start,
source.cost_per_conv_1d_click,
source.cost_per_conv_1d_view,
source.cost_per_conv_28d_click,
source.cost_per_conv_28d_view,
source.cost_per_conv_7d_click,
source.cost_per_conv_7d_view,
source.cost_per_conv_action_type,
source.cost_per_conv_value
)
