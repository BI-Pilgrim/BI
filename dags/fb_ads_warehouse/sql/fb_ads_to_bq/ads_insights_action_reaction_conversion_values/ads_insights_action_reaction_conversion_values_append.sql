MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_conversion_values` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,


    -- conversion_values,
    json_extract_scalar(con_val, '$.1d_click') AS conversion_values_1d_click,
    json_extract_scalar(con_val, '$.1d_view') AS conversion_values_1d_view,
    json_extract_scalar(con_val, '$.28d_click') AS conversion_values_28d_click,
    json_extract_scalar(con_val, '$.28d_view') AS conversion_values_28d_view,
    json_extract_scalar(con_val, '$.7d_click') AS conversion_values_7d_click,
    json_extract_scalar(con_val, '$.7d_view') AS conversion_values_7d_view,
    json_extract_scalar(con_val, '$.action_type') AS conversion_values_action_type,
    json_extract_scalar(con_val, '$.value') AS conversion_values_value,
  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction,
        UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS con_val
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
    TARGET.conversion_values_1d_click = SOURCE.conversion_values_1d_click,
    TARGET.conversion_values_1d_view = SOURCE.conversion_values_1d_view,
    TARGET.conversion_values_28d_click = SOURCE.conversion_values_28d_click,
    TARGET.conversion_values_28d_view = SOURCE.conversion_values_28d_view,
    TARGET.conversion_values_7d_click = SOURCE.conversion_values_7d_click,
    TARGET.conversion_values_7d_view = SOURCE.conversion_values_7d_view,
    TARGET.conversion_values_action_type = SOURCE.conversion_values_action_type,
    TARGET.conversion_values_value = SOURCE.conversion_values_value
WHEN NOT MATCHED
THEN INSERT
(
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,
    conversion_values_1d_click,
    conversion_values_1d_view,
    conversion_values_28d_click,
    conversion_values_28d_view,
    conversion_values_7d_click,
    conversion_values_7d_view,
    conversion_values_action_type,
    conversion_values_value
)
VALUES
(
    SOURCE._airbyte_extracted_at,
    SOURCE.ad_id,
    SOURCE.adset_id,
    SOURCE.account_id,
    SOURCE.campaign_id,
    SOURCE.conversion_values_1d_click,
    SOURCE.conversion_values_1d_view,
    SOURCE.conversion_values_28d_click,
    SOURCE.conversion_values_28d_view,
    SOURCE.conversion_values_7d_click,
    SOURCE.conversion_values_7d_view,
    SOURCE.conversion_values_action_type,
    SOURCE.conversion_values_value
)
