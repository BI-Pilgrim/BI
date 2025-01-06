MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_conversion_values` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,


    -- conversion_values,
    JSON_EXTRACT_SCALAR(conv_values, '$.1d_click') AS conv_values_1d_click, --400(B)
    JSON_EXTRACT_SCALAR(conv_values, '$.28d_click') AS conv_values_28d_click,
    JSON_EXTRACT_SCALAR(conv_values, '$.7d_click') AS conv_values_7d_click,
    JSON_EXTRACT_SCALAR(conv_values, '$.action_type') AS conv_values_action_type,
    JSON_EXTRACT_SCALAR(conv_values, '$.value') AS conv_values_value,
    FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
        UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS conv_values
      WHERE
        DATE(_airbyte_extracted_at) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
    WHERE row_nuM = 1  
) AS SOURCE


ON TARGET.ad_id = SOURCE.ad_id
WHEN MATCHED AND TARGET._airbyte_extracted_at > SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.ad_id = SOURCE.ad_id,
  TARGET.adset_id = SOURCE.adset_id,
  TARGET.account_id = SOURCE.account_id,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.conv_values_1d_click = SOURCE.conv_values_1d_click,
  TARGET.conv_values_28d_click = SOURCE.conv_values_28d_click,
  TARGET.conv_values_7d_click = SOURCE.conv_values_7d_click,
  TARGET.conv_values_action_type = SOURCE.conv_values_action_type,
  TARGET.conv_values_value = SOURCE.conv_values_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  conv_values_1d_click,
  conv_values_28d_click,
  conv_values_7d_click,
  conv_values_action_type,
  conv_values_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.conv_values_1d_click,
  SOURCE.conv_values_28d_click,
  SOURCE.conv_values_7d_click,
  SOURCE.conv_values_action_type,
  SOURCE.conv_values_value
)
