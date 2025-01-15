MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_carousel_card_mobile_app_purchase_roas` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,


    -- mobile_app_purchase_roas,
    JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.28d_click') AS mob_app_purchase_roas_28d_click, --11K(E)
    JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.28d_view') AS mob_app_purchase_roas_28d_view,
    JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.7d_click') AS mob_app_purchase_roas_7d_click,
    JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.7d_click') AS mob_app_purchase_roas_7d_view,
    JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.1d_click') AS mob_app_purchase_roas_1d_click,
    JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.1d_view') AS mob_app_purchase_roas_1d_view,
    JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.action_type') AS mob_app_purchase_roas_action_type,
    JSON_EXTRACT_SCALAR(mob_app_purchase_roas, '$.value') AS mob_app_purchase_roas_value,
    FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_carousel_card,
        UNNEST(JSON_EXTRACT_ARRAY(mobile_app_purchase_roas)) AS mob_app_purchase_roas  
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
  TARGET.mob_app_purchase_roas_28d_click = SOURCE.mob_app_purchase_roas_28d_click,
  TARGET.mob_app_purchase_roas_28d_view = SOURCE.mob_app_purchase_roas_28d_view,
  TARGET.mob_app_purchase_roas_7d_click = SOURCE.mob_app_purchase_roas_7d_click,
  TARGET.mob_app_purchase_roas_7d_view = SOURCE.mob_app_purchase_roas_7d_view,
  TARGET.mob_app_purchase_roas_1d_click = SOURCE.mob_app_purchase_roas_1d_click,
  TARGET.mob_app_purchase_roas_1d_view = SOURCE.mob_app_purchase_roas_1d_view,
  TARGET.mob_app_purchase_roas_action_type = SOURCE.mob_app_purchase_roas_action_type,
  TARGET.mob_app_purchase_roas_value = SOURCE.mob_app_purchase_roas_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  mob_app_purchase_roas_28d_click,
  mob_app_purchase_roas_28d_view,
  mob_app_purchase_roas_7d_click,
  mob_app_purchase_roas_7d_view,
  mob_app_purchase_roas_1d_click,
  mob_app_purchase_roas_1d_view,
  mob_app_purchase_roas_action_type,
  mob_app_purchase_roas_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.mob_app_purchase_roas_28d_click,
  SOURCE.mob_app_purchase_roas_28d_view,
  SOURCE.mob_app_purchase_roas_7d_click,
  SOURCE.mob_app_purchase_roas_7d_view,
  SOURCE.mob_app_purchase_roas_1d_click,
  SOURCE.mob_app_purchase_roas_1d_view,
  SOURCE.mob_app_purchase_roas_action_type,
  SOURCE.mob_app_purchase_roas_value
)
