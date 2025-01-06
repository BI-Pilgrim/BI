MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_unique_actions_n_conversions_n_web_ctr` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,


    -- conversions,
    JSON_EXTRACT_SCALAR(CON1, '$.28d_click') AS conversions_28d_click, --30K(A)
    JSON_EXTRACT_SCALAR(CON1, '$.28d_view') AS conversions_28d_view,
    JSON_EXTRACT_SCALAR(CON1, '$.7d_click') AS conversions_7d_click,
    JSON_EXTRACT_SCALAR(CON1, '$.7d_click') AS conversions_7d_view,
    JSON_EXTRACT_SCALAR(CON1, '$.1d_click') AS conversions_1d_click,
    JSON_EXTRACT_SCALAR(CON1, '$.1d_view') AS conversions_1d_view,
    JSON_EXTRACT_SCALAR(CON1, '$.action_type') AS conversions_action_type,
    JSON_EXTRACT_SCALAR(CON1, '$.value') AS conversions_value,


    -- website_ctr,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(conversions, '$[0]'), '$.action_type') AS action_type, --30K(A)
    CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT(conversions, '$[0]'), '$.value') AS FLOAT64) AS value,


    -- unique_actions,
    JSON_EXTRACT_SCALAR(unique_act, '$.1d_click') AS unique_act_1d_click, --30K(A)
    JSON_EXTRACT_SCALAR(unique_act, '$.28d_click') AS unique_act_28d_click,
    JSON_EXTRACT_SCALAR(unique_act, '$.7d_click') AS unique_act_7d_click,
    JSON_EXTRACT_SCALAR(unique_act, '$.action_type') AS unique_act_action_type,
    JSON_EXTRACT_SCALAR(unique_act, '$.value') AS unique_act_value,
    FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
        UNNEST(JSON_EXTRACT_ARRAY(conversions)) AS CON1,
        UNNEST(JSON_EXTRACT_ARRAY(unique_actions)) AS unique_act  
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
  TARGET.conversions_28d_click = SOURCE.conversions_28d_click,
  TARGET.conversions_28d_view = SOURCE.conversions_28d_view,
  TARGET.conversions_7d_click = SOURCE.conversions_7d_click,
  TARGET.conversions_7d_view = SOURCE.conversions_7d_view,
  TARGET.conversions_1d_click = SOURCE.conversions_1d_click,
  TARGET.conversions_1d_view = SOURCE.conversions_1d_view,
  TARGET.conversions_action_type = SOURCE.conversions_action_type,
  TARGET.conversions_value = SOURCE.conversions_value,
  TARGET.action_type = SOURCE.action_type,
  TARGET.value = SOURCE.value,
  TARGET.unique_act_1d_click = SOURCE.unique_act_1d_click,
  TARGET.unique_act_28d_click = SOURCE.unique_act_28d_click,
  TARGET.unique_act_7d_click = SOURCE.unique_act_7d_click,
  TARGET.unique_act_action_type = SOURCE.unique_act_action_type,
  TARGET.unique_act_value = SOURCE.unique_act_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  conversions_28d_click,
  conversions_28d_view,
  conversions_7d_click,
  conversions_7d_view,
  conversions_1d_click,
  conversions_1d_view,
  conversions_action_type,
  conversions_value,
  action_type,
  value,
  unique_act_1d_click,
  unique_act_28d_click,
  unique_act_7d_click,
  unique_act_action_type,
  unique_act_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.conversions_28d_click,
  SOURCE.conversions_28d_view,
  SOURCE.conversions_7d_click,
  SOURCE.conversions_7d_view,
  SOURCE.conversions_1d_click,
  SOURCE.conversions_1d_view,
  SOURCE.conversions_action_type,
  SOURCE.conversions_value,
  SOURCE.action_type,
  SOURCE.value,
  SOURCE.unique_act_1d_click,
  SOURCE.unique_act_28d_click,
  SOURCE.unique_act_7d_click,
  SOURCE.unique_act_action_type,
  SOURCE.unique_act_value
)
