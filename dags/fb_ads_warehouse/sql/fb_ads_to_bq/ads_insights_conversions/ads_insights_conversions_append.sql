MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_conversions` AS TARGET
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

  JSON_EXTRACT_SCALAR(conv, '$.1d_click') AS conv_1d_click,
  JSON_EXTRACT_SCALAR(conv, '$.1d_view') AS conv_1d_view,
  JSON_EXTRACT_SCALAR(conv, '$.28d_click') AS conv_28d_click,
  JSON_EXTRACT_SCALAR(conv, '$.28d_view') AS conv_28d_view,
  JSON_EXTRACT_SCALAR(conv, '$.7d_click') AS conv_7d_click,
  JSON_EXTRACT_SCALAR(conv, '$.7d_view') AS conv_7d_view,
  JSON_EXTRACT_SCALAR(conv, '$.action_type') AS conv_action_type,
  JSON_EXTRACT_SCALAR(conv, '$.value') AS conv_value,
  row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(conv, '$.action_type') order by _airbyte_extracted_at desc) as rn
FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`,
  UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conv
)
where rn = 1
) AS SOURCE
ON TARGET.ad_id = SOURCE.ad_id
and TARGET.date_start = SOURCE.date_start
and TARGET.conv_action_type = SOURCE.conv_action_type
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at 
THEN UPDATE SET
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.campaign_id = source.campaign_id,
target.adset_id = source.adset_id,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.conv_1d_click = source.conv_1d_click,  
target.conv_1d_view = source.conv_1d_view,  
target.conv_28d_click = source.conv_28d_click,  
target.conv_28d_view = source.conv_28d_view,  
target.conv_7d_click = source.conv_7d_click,  
target.conv_7d_view = source.conv_7d_view,  
target.conv_action_type = source.conv_action_type,  
target.conv_value = source.conv_value 
WHEN NOT MATCHED
THEN INSERT
(
_airbyte_extracted_at,
campaign_id,
adset_id,
ad_id,
date_start,
conv_1d_click,
conv_1d_view,
conv_28d_click,
conv_28d_view,
conv_7d_click,
conv_7d_view,
conv_action_type,
conv_value
)
VALUES
(
source._airbyte_extracted_at,
source.campaign_id,
source.adset_id,
source.ad_id,
source.date_start,
source.conv_1d_click,
source.conv_1d_view,
source.conv_28d_click,
source.conv_28d_view,
source.conv_7d_click,
source.conv_7d_view,
source.conv_action_type,
source.conv_value
)
