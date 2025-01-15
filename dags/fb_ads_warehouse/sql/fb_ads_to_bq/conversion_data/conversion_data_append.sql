MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.conversion_data` AS TARGET
USING
(
  SELECT
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  JSON_EXTRACT_SCALAR(ctr1, '$.action_type') as website_ctr_action_type,
  JSON_EXTRACT_SCALAR(ctr1, '$.value') as website_ctr_value,
  JSON_EXTRACT_SCALAR(conversions1, '$.28d_click') AS conversions_28d_click,
  JSON_EXTRACT_SCALAR(conversions1, '$.7d_click') AS conversions_7d_click,
  JSON_EXTRACT_SCALAR(conversions1, '$.action_type') AS conversions_action_type,
  JSON_EXTRACT_SCALAR(conversions1, '$.value') AS conversions_value,
  JSON_EXTRACT_SCALAR(conv, '$.1d_click') AS conv_1d_click,
  JSON_EXTRACT_SCALAR(conv, '$.1d_view') AS conv_1d_view,
  JSON_EXTRACT_SCALAR(conv, '$.28d_click') AS conv_28d_click,
  JSON_EXTRACT_SCALAR(conv, '$.28d_view') AS conv_28d_view,
  JSON_EXTRACT_SCALAR(conv, '$.7d_click') AS conv_7d_click,
  JSON_EXTRACT_SCALAR(conv, '$.7d_view') AS conv_7d_view,
  JSON_EXTRACT_SCALAR(conv, '$.action_type') AS conv_action_type,
  JSON_EXTRACT_SCALAR(conv, '$.value') AS conv_value,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.1d_click') AS cost_per_conv_1d_click,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.1d_view') AS cost_per_conv_1d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.28d_click') AS cost_per_conv_28d_click,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.28d_view') AS cost_per_conv_28d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.7d_click') AS cost_per_conv_7d_click,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.7d_view') AS cost_per_conv_7d_view,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.action_type') AS cost_per_conv_action_type,
  JSON_EXTRACT_SCALAR(cost_per_conv, '$.value') AS cost_per_conv_value,
  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY _airbyte_extracted_at ORDER BY _airbyte_extracted_at) AS row_num
    FROM
      shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
      UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS conversions1,
      UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS conv,
      UNNEST(JSON_EXTRACT_ARRAY(cost_per_conversion)) AS cost_per_conv,
      UNNEST(JSON_EXTRACT_ARRAY(website_ctr)) AS ctr1
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1
) AS SOURCE
ON TARGET.adset_id = SOURCE.adset_id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at


THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.adset_id = SOURCE.adset_id,
  TARGET.ad_id = SOURCE.ad_id,
  TARGET.website_ctr_action_type = SOURCE.website_ctr_action_type,
  TARGET.website_ctr_value = SOURCE.website_ctr_value,
  TARGET.conversions_28d_click = SOURCE.conversions_28d_click,
  TARGET.conversions_7d_click = SOURCE.conversions_7d_click,
  TARGET.conversions_action_type = SOURCE.conversions_action_type,
  TARGET.conversions_value = SOURCE.conversions_value,
  TARGET.conv_1d_click = SOURCE.conv_1d_click,
  TARGET.conv_1d_view = SOURCE.conv_1d_view,
  TARGET.conv_28d_click = SOURCE.conv_28d_click,
  TARGET.conv_28d_view = SOURCE.conv_28d_view,
  TARGET.conv_7d_click = SOURCE.conv_7d_click,
  TARGET.conv_7d_view = SOURCE.conv_7d_view,
  TARGET.conv_action_type = SOURCE.conv_action_type,
  TARGET.conv_value = SOURCE.conv_value,
  TARGET.cost_per_conv_1d_click = SOURCE.cost_per_conv_1d_click,
  TARGET.cost_per_conv_1d_view = SOURCE.cost_per_conv_1d_view,
  TARGET.cost_per_conv_28d_click = SOURCE.cost_per_conv_28d_click,
  TARGET.cost_per_conv_28d_view = SOURCE.cost_per_conv_28d_view,
  TARGET.cost_per_conv_7d_click = SOURCE.cost_per_conv_7d_click,
  TARGET.cost_per_conv_7d_view = SOURCE.cost_per_conv_7d_view,
  TARGET.cost_per_conv_action_type = SOURCE.cost_per_conv_action_type,
  TARGET.cost_per_conv_value = SOURCE.cost_per_conv_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  website_ctr_action_type,
  website_ctr_value,
  conversions_28d_click,
  conversions_7d_click,
  conversions_action_type,
  conversions_value,
  conv_1d_click,
  conv_1d_view,
  conv_28d_click,
  conv_28d_view,
  conv_7d_click,
  conv_7d_view,
  conv_action_type,
  conv_value,
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
  SOURCE._airbyte_extracted_at,
  SOURCE.campaign_id,
  SOURCE.adset_id,
  SOURCE.ad_id,
  SOURCE.website_ctr_action_type,
  SOURCE.website_ctr_value,
  SOURCE.conversions_28d_click,
  SOURCE.conversions_7d_click,
  SOURCE.conversions_action_type,
  SOURCE.conversions_value,
  SOURCE.conv_1d_click,
  SOURCE.conv_1d_view,
  SOURCE.conv_28d_click,
  SOURCE.conv_28d_view,
  SOURCE.conv_7d_click,
  SOURCE.conv_7d_view,
  SOURCE.conv_action_type,
  SOURCE.conv_value,
  SOURCE.cost_per_conv_1d_click,
  SOURCE.cost_per_conv_1d_view,
  SOURCE.cost_per_conv_28d_click,
  SOURCE.cost_per_conv_28d_view,
  SOURCE.cost_per_conv_7d_click,
  SOURCE.cost_per_conv_7d_view,
  SOURCE.cost_per_conv_action_type,
  SOURCE.cost_per_conv_value
)
