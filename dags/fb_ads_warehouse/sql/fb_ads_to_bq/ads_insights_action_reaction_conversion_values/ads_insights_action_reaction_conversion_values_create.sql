CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_conversion_values`
AS
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
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction,
  UNNEST(JSON_EXTRACT_ARRAY(conversion_values)) AS con_val