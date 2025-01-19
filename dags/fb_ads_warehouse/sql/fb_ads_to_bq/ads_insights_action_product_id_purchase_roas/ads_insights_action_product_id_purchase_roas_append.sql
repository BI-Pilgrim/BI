MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_action_product_id_purchase_roas` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_id,
    adset_id,
    account_id,
    campaign_id,


    -- purchase_roas,
    JSON_EXTRACT_SCALAR(purch_roas, '$.1d_click') AS purchase_roas_1d_click,
    JSON_EXTRACT_SCALAR(purch_roas, '$.1d_view') AS purchase_roas_1d_view,
    JSON_EXTRACT_SCALAR(purch_roas, '$.28d_click') AS purchase_roas_28d_click,
    JSON_EXTRACT_SCALAR(purch_roas, '$.28d_views') AS purchase_roas_28d_views,
    JSON_EXTRACT_SCALAR(purch_roas, '$.7d_click') AS purchase_roas_7d_click,
    JSON_EXTRACT_SCALAR(purch_roas, '$.7d_view') AS purchase_roas_7d_view,
    JSON_EXTRACT_SCALAR(purch_roas, '$.action_type') AS purchase_roas_action_type,
    JSON_EXTRACT_SCALAR(purch_roas, '$.value') AS purchase_roas_value,
    FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY adset_id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights_action_product_id,
        UNNEST(JSON_EXTRACT_ARRAY(purchase_roas)) AS purch_roas
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
  TARGET.purchase_roas_1d_click = SOURCE.purchase_roas_1d_click,
  TARGET.purchase_roas_1d_view = SOURCE.purchase_roas_1d_view,
  TARGET.purchase_roas_28d_click = SOURCE.purchase_roas_28d_click,
  TARGET.purchase_roas_28d_views = SOURCE.purchase_roas_28d_views,
  TARGET.purchase_roas_7d_click = SOURCE.purchase_roas_7d_click,
  TARGET.purchase_roas_7d_view = SOURCE.purchase_roas_7d_view,
  TARGET.purchase_roas_action_type = SOURCE.purchase_roas_action_type,
  TARGET.purchase_roas_value = SOURCE.purchase_roas_value
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_id,
  adset_id,
  account_id,
  campaign_id,
  purchase_roas_1d_click,
  purchase_roas_1d_view,
  purchase_roas_28d_click,
  purchase_roas_28d_views,
  purchase_roas_7d_click,
  purchase_roas_7d_view,
  purchase_roas_action_type,
  purchase_roas_value
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_id,
  SOURCE.adset_id,
  SOURCE.account_id,
  SOURCE.campaign_id,
  SOURCE.purchase_roas_1d_click,
  SOURCE.purchase_roas_1d_view,
  SOURCE.purchase_roas_28d_click,
  SOURCE.purchase_roas_28d_views,
  SOURCE.purchase_roas_7d_click,
  SOURCE.purchase_roas_7d_view,
  SOURCE.purchase_roas_action_type,
  SOURCE.purchase_roas_value
)
