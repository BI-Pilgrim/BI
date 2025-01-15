MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_country_unique_actions` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    campaign_id,
    account_id,


    -- unique_actions,
    JSON_EXTRACT_SCALAR(unique_acts, '$.1d_click') AS unique_actions_1d_click,
    JSON_EXTRACT_SCALAR(unique_acts, '$.1d_view') AS unique_actions_1d_view,
    JSON_EXTRACT_SCALAR(unique_acts, '$.28d_click') AS unique_actions_28d_click,
    JSON_EXTRACT_SCALAR(unique_acts, '$.28d_click') AS unique_actions_28d_view,
    JSON_EXTRACT_SCALAR(unique_acts, '$.7d_click') AS unique_actions_7d_click,
    JSON_EXTRACT_SCALAR(unique_acts, '$.7d_view') AS unique_actions_7d_view,
    JSON_EXTRACT_SCALAR(unique_acts, '$.action_type') AS unique_actions_action_type,
    JSON_EXTRACT_SCALAR(unique_acts, '$.value') AS unique_actions_value,


  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_country,
        UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_acts
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
  TARGET.unique_actions_1d_click = SOURCE.unique_actions_1d_click,
  TARGET.unique_actions_1d_view = SOURCE.unique_actions_1d_view,
  TARGET.unique_actions_28d_click = SOURCE.unique_actions_28d_click,
  TARGET.unique_actions_28d_view = SOURCE.unique_actions_28d_view,
  TARGET.unique_actions_7d_click = SOURCE.unique_actions_7d_click,
  TARGET.unique_actions_7d_view = SOURCE.unique_actions_7d_view,
  TARGET.unique_actions_action_type = SOURCE.unique_actions_action_type,
  TARGET.unique_actions_value = SOURCE.unique_actions_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  unique_actions_1d_click,
  unique_actions_1d_view,
  unique_actions_28d_click,
  unique_actions_28d_view,
  unique_actions_7d_click,
  unique_actions_7d_view,
  unique_actions_action_type,
  unique_actions_value


)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.unique_actions_1d_click,
  SOURCE.unique_actions_1d_view,
  SOURCE.unique_actions_7d_click,
  SOURCE.unique_actions_7d_view,
  SOURCE.unique_actions_28d_click,
  SOURCE.unique_actions_28d_click,
  SOURCE.unique_actions_action_type,
  SOURCE.unique_actions_value
)
