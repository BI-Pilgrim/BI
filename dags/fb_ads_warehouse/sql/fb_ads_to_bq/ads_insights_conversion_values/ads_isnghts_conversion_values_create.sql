CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_isnghts_conversion_values`
PARTITION BY DATE_TRUNC(_airbyte_extracted_at, DAY)
AS
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
  -- JSON_EXTRACT_SCALAR(conv, '$.1d_click') AS conv_1d_click,
  -- JSON_EXTRACT_SCALAR(conv, '$.1d_view') AS conv_1d_view,
  -- JSON_EXTRACT_SCALAR(conv, '$.28d_click') AS conv_28d_click,
  -- JSON_EXTRACT_SCALAR(conv, '$.28d_view') AS conv_28d_view,
  -- JSON_EXTRACT_SCALAR(conv, '$.7d_click') AS conv_7d_click,
  -- JSON_EXTRACT_SCALAR(conv, '$.7d_view') AS conv_7d_view,
  -- JSON_EXTRACT_SCALAR(conv, '$.action_type') AS conv_action_type,
  -- JSON_EXTRACT_SCALAR(conv, '$.value') AS conv_value,
  -- JSON_EXTRACT_SCALAR(cost_per_conv, '$.1d_click') AS cost_per_conv_1d_click,
  -- JSON_EXTRACT_SCALAR(cost_per_conv, '$.1d_view') AS cost_per_conv_1d_view,
  -- JSON_EXTRACT_SCALAR(cost_per_conv, '$.28d_click') AS cost_per_conv_28d_click,
  -- JSON_EXTRACT_SCALAR(cost_per_conv, '$.28d_view') AS cost_per_conv_28d_view,
  -- JSON_EXTRACT_SCALAR(cost_per_conv, '$.7d_click') AS cost_per_conv_7d_click,
  -- JSON_EXTRACT_SCALAR(cost_per_conv, '$.7d_view') AS cost_per_conv_7d_view,
  -- JSON_EXTRACT_SCALAR(cost_per_conv, '$.action_type') AS cost_per_conv_action_type,
  -- JSON_EXTRACT_SCALAR(cost_per_conv, '$.value') AS cost_per_conv_value,
  row_number() over(partition by ad_id,date_start,JSON_EXTRACT_SCALAR(conversions, '$.action_type') order by _airbyte_extracted_at desc) as rn
FROM
  `shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights`,
  UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS conversions
  -- UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conv,
  -- UNNEST(JSON_EXTRACT_ARRAY(cost_per_conversion)) AS cost_per_conv,
  -- UNNEST(JSON_EXTRACT_ARRAY(website_ctr)) AS ctr
)
where rn = 1