MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_type_cost_per_unique_action_type` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    campaign_id,


    -- cost_per_unique_action_type,
    JSON_EXTRACT_SCALAR(cpuat, '$.1d_click') AS cost_per_unique_action_type_1d_click,
    JSON_EXTRACT_SCALAR(cpuat, '$.1d_view') AS cost_per_unique_action_type_1d_view,
    JSON_EXTRACT_SCALAR(cpuat, '$.28d_click') AS cost_per_unique_action_type_28d_click,
    JSON_EXTRACT_SCALAR(cpuat, '$.7d_click') AS cost_per_unique_action_type_7d_click,
    JSON_EXTRACT_SCALAR(cpuat, '$.7d_view') AS cost_per_unique_action_type_7d_view,
    JSON_EXTRACT_SCALAR(cpuat, '$.action_type') AS cost_per_unique_action_type_action_type,
    JSON_EXTRACT_SCALAR(cpuat, '$.value') AS cost_per_unique_action_type_value,


  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_type,
        UNNEST(JSON_EXTRACT_ARRAY(cost_per_unique_action_type)) AS cpuat
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
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.cost_per_unique_action_type_1d_click = SOURCE.cost_per_unique_action_type_1d_click,
  TARGET.cost_per_unique_action_type_1d_view = SOURCE.cost_per_unique_action_type_1d_view,
  TARGET.cost_per_unique_action_type_28d_click = SOURCE.cost_per_unique_action_type_28d_click,
  TARGET.cost_per_unique_action_type_7d_click = SOURCE.cost_per_unique_action_type_7d_click,
  TARGET.cost_per_unique_action_type_7d_view = SOURCE.cost_per_unique_action_type_7d_view,
  TARGET.cost_per_unique_action_type_action_type = SOURCE.cost_per_unique_action_type_action_type,
  TARGET.cost_per_unique_action_type_value = SOURCE.cost_per_unique_action_type_value  
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  campaign_id,
  cost_per_unique_action_type_1d_click,
  cost_per_unique_action_type_1d_view,
  cost_per_unique_action_type_28d_click,
  cost_per_unique_action_type_7d_click,
  cost_per_unique_action_type_7d_view,
  cost_per_unique_action_type_action_type,
  cost_per_unique_action_type_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.campaign_id,
  SOURCE.cost_per_unique_action_type_1d_click,
  SOURCE.cost_per_unique_action_type_1d_view,
  SOURCE.cost_per_unique_action_type_28d_click,
  SOURCE.cost_per_unique_action_type_7d_click,
  SOURCE.cost_per_unique_action_type_7d_view,
  SOURCE.cost_per_unique_action_type_action_type,
  SOURCE.cost_per_unique_action_type_value
)
