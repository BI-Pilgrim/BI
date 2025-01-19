MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_actions` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,


    -- actions,
    json_extract_scalar(acts, '$.1d_click') as actions_1d_click,
    json_extract_scalar(acts, '$.1d_view') as actions_1d_view,
    json_extract_scalar(acts, '$.28d_click') as actions_28d_click,
    json_extract_scalar(acts, '$.28d_view') as actions_28d_view,
    json_extract_scalar(acts, '$.7d_click') as actions_7d_click,
    json_extract_scalar(acts, '$.7d_view') as actions_7d_view,
    json_extract_scalar(acts, '$.action_type') as actions_action_type,
    json_extract_scalar(acts, '$.value') as actions_value,
  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction,
        UNNEST(JSON_EXTRACT_ARRAY(actions)) AS acts
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
    TARGET.actions_1d_click = SOURCE.actions_1d_click,
    TARGET.actions_1d_view = SOURCE.actions_1d_view,
    TARGET.actions_28d_click = SOURCE.actions_28d_click,
    TARGET.actions_28d_view = SOURCE.actions_28d_view,
    TARGET.actions_7d_click = SOURCE.actions_7d_click,
    TARGET.actions_7d_view = SOURCE.actions_7d_view,
    TARGET.actions_action_type = SOURCE.actions_action_type,
    TARGET.actions_value = SOURCE.actions_value
WHEN NOT MATCHED
THEN INSERT
(
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,
    actions_1d_click,
    actions_1d_view,
    actions_28d_click,
    actions_28d_view,
    actions_7d_click,
    actions_7d_view,
    actions_action_type,
    actions_value
)
VALUES
(
    SOURCE._airbyte_extracted_at,
    SOURCE.ad_id,
    SOURCE.adset_id,
    SOURCE.account_id,
    SOURCE.campaign_id,
    SOURCE.actions_1d_click,
    SOURCE.actions_1d_view,
    SOURCE.actions_28d_click,
    SOURCE.actions_28d_view,
    SOURCE.actions_7d_click,
    SOURCE.actions_7d_view,
    SOURCE.actions_action_type,
    SOURCE.actions_value
)
