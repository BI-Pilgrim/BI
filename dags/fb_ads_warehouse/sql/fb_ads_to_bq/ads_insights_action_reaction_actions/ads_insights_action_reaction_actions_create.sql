CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_reaction_actions`
AS
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
  shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_reaction,
  UNNEST(JSON_EXTRACT_ARRAY(actions)) AS acts
