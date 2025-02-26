MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_isnghts_conversion_values` AS TARGET
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
  JSON_EXTRACT_SCALAR(conversions, '$.1d_click') AS conversions_1d_click,
  JSON_EXTRACT_SCALAR(conversions, '$.1d_ciew') AS conversions_1d_ciew,
  JSON_EXTRACT_SCALAR(conversions, '$.28d_click') AS conversions_28d_click,
  JSON_EXTRACT_SCALAR(conversions, '$.28d_view') AS conversions_28d_view,
  JSON_EXTRACT_SCALAR(conversions, '$.7d_click') AS conversions_7d_click,
  JSON_EXTRACT_SCALAR(conversions, '$.7d_view') AS conversions_7d_view,
  JSON_EXTRACT_SCALAR(conversions, '$.action_type') AS conversions_action_type,
  JSON_EXTRACT_SCALAR(conversions, '$.value') AS conversions_value,
  row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(conversions, '$.action_type') order by _airbyte_extracted_at desc) as rn
FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`,
  UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS conversions
)
where rn = 1
) AS SOURCE
ON TARGET.ad_id = SOURCE.ad_id
and TARGET.date_start = SOURCE.date_start
and TARGET.conversions_action_type = SOURCE.conversions_action_type
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at 
THEN UPDATE SET
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.campaign_id = source.campaign_id,
target.adset_id = source.adset_id,
target.ad_id = source.ad_id,
target.date_start = source.date_start,
target.conversions_1d_click = source.conversions_1d_click,  
target.conversions_1d_ciew = source.conversions_1d_ciew,  
target.conversions_28d_click = source.conversions_28d_click,  
target.conversions_28d_view = source.conversions_28d_view,  
target.conversions_7d_click = source.conversions_7d_click,  
target.conversions_7d_view = source.conversions_7d_view,  
target.conversions_action_type = source.conversions_action_type,  
target.conversions_value = source.conversions_value 
WHEN NOT MATCHED
THEN INSERT
(
_airbyte_extracted_at,
campaign_id,
adset_id,
ad_id,
date_start,
conversions_1d_click,
conversions_1d_ciew,
conversions_28d_click,
conversions_28d_view,
conversions_7d_click,
conversions_7d_view,
conversions_action_type,
conversions_value
)
VALUES
(
source._airbyte_extracted_at,
source.campaign_id,
source.adset_id,
source.ad_id,
source.date_start,
source.conversions_1d_click,
source.conversions_1d_ciew,
source.conversions_28d_click,
source.conversions_28d_view,
source.conversions_7d_click,
source.conversions_7d_view,
source.conversions_action_type,
source.conversions_value
)
