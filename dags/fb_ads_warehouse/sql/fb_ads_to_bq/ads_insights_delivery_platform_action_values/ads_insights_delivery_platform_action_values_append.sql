MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_delivery_platform_action_values` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,


    -- action_values,
    JSON_EXTRACT_SCALAR(act_values, '$.1d_click') AS action_values_1d_click,
    JSON_EXTRACT_SCALAR(act_values, '$.1d_view') AS action_values_1d_view,
    JSON_EXTRACT_SCALAR(act_values, '$.28d_click') AS action_values_28d_click,
    JSON_EXTRACT_SCALAR(act_values, '$.28d_views') AS action_values_28d_views,  
    JSON_EXTRACT_SCALAR(act_values, '$.7d_click') AS action_values_7d_click,
    JSON_EXTRACT_SCALAR(act_values, '$.7d_view') AS action_values_7d_view,
    JSON_EXTRACT_SCALAR(act_values, '$.action_type') AS action_values_action_type,
    JSON_EXTRACT_SCALAR(act_values, '$.value') AS action_values_value,


  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_delivery_device,
        UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_values
      WHERE
        DATE(_airbyte_extracted_at) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
  WHERE row_num = 1  
) AS SOURCE


ON TARGET.ad_id = SOURCE.ad_id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.ad_id = SOURCE.ad_id,
  TARGET.adset_id = SOURCE.adset_id,
  TARGET.account_id = SOURCE.account_id,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.action_values_1d_click = SOURCE.action_values_1d_click,
  TARGET.action_values_1d_view = SOURCE.action_values_1d_view,
  TARGET.action_values_28d_click = SOURCE.action_values_28d_click,
  TARGET.action_values_28d_views = SOURCE.action_values_28d_views,
  TARGET.action_values_7d_click = SOURCE.action_values_7d_click,
  TARGET.action_values_7d_view = SOURCE.action_values_7d_view,
  TARGET.action_values_action_type = SOURCE.action_values_action_type,
  TARGET.action_values_value = SOURCE.action_values_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  action_values_1d_click,
  action_values_1d_view,
  action_values_28d_click,
  action_values_28d_views,
  action_values_7d_click,
  action_values_7d_view,
  action_values_action_type,
  action_values_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.action_values_1d_click,
  SOURCE.action_values_1d_view,
  SOURCE.action_values_28d_click,
  SOURCE.action_values_28d_views,
  SOURCE.action_values_7d_click,
  SOURCE.action_values_7d_view,
  SOURCE.action_values_action_type,
  SOURCE.action_values_value
)
