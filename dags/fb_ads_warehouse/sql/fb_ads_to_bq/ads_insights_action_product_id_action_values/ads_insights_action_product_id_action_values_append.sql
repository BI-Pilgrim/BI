MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_action_values` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,


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
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id,
        UNNEST(JSON_EXTRACT_ARRAY(action_values)) AS act_values
      WHERE
        DATE(_airbyte_extracted_at) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
    WHERE row_nuM = 1  
) AS SOURCE


ON TARGET.ad_id = SOURCE.ad_id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.ad_id = SOURCE.ad_id,
  TARGET.adset_id = SOURCE.adset_id,
  TARGET.account_id = SOURCE.account_id,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.act_values_1d_click = SOURCE.act_values_1d_click,
  TARGET.act_values_1d_view = SOURCE.act_values_1d_view,
  TARGET.act_values_28d_click = SOURCE.act_values_28d_click,
  TARGET.act_values_28d_views = SOURCE.act_values_28d_views,
  TARGET.act_values_7d_click = SOURCE.act_values_7d_click,
  TARGET.act_values_7d_view = SOURCE.act_values_7d_view,
  TARGET.act_values_action_type = SOURCE.act_values_action_type,
  TARGET.act_values_value = SOURCE.act_values_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  act_values_1d_click,
  act_values_1d_view,
  act_values_28d_click,
  act_values_28d_views,
  act_values_7d_click,
  act_values_7d_view,
  act_values_action_type,
  act_values_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.act_values_1d_click,
  SOURCE.act_values_1d_view,
  SOURCE.act_values_28d_click,
  SOURCE.act_values_28d_views,
  SOURCE.act_values_7d_click,
  SOURCE.act_values_7d_view,
  SOURCE.act_values_action_type,
  SOURCE.act_values_value
)
