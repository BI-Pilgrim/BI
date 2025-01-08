MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.roas` AS TARGET
USING (
  SELECT
    _airbyte_extracted_at,
    campaign_id,
    adset_id,
    ad_id,
    JSON_EXTRACT_SCALAR(roas, '$.1d_click') AS one_day_click_roas,
    JSON_EXTRACT_SCALAR(roas, '$.1d_view') AS one_day_view_roas,
    JSON_EXTRACT_SCALAR(roas, '$.28d_click') AS twentyeight_day_click_roas,
    JSON_EXTRACT_SCALAR(roas, '$.28d_view') AS twentyeight_day_view_roas,
    JSON_EXTRACT_SCALAR(roas, '$.7d_click') AS seven_day_click_roas,
    JSON_EXTRACT_SCALAR(roas, '$.7d_view') AS seven_day_view_roas,
    JSON_EXTRACT_SCALAR(roas, '$.action_type') AS roas_action_type,
    JSON_EXTRACT_SCALAR(roas, '$.value') AS value_roas,
    JSON_EXTRACT_SCALAR(p_roas, '$.1d_click') AS one_day_click_purchase_roas,
    JSON_EXTRACT_SCALAR(p_roas, '$.1d_view') AS one_day_view_purchase_roas,
    JSON_EXTRACT_SCALAR(p_roas, '$.28d_click') AS twentyeight_day_click_purchase_roas,
    JSON_EXTRACT_SCALAR(p_roas, '$.28d_view') AS twentyeight_day_view_purchase_roas,
    JSON_EXTRACT_SCALAR(p_roas, '$.7d_click') AS seven_day_click_purchase_roas,
    JSON_EXTRACT_SCALAR(p_roas, '$.7d_view') AS seven_day_view_purchase_roas,
    JSON_EXTRACT_SCALAR(p_roas, '$.action_type') AS purchase_roas_action_type,
    JSON_EXTRACT_SCALAR(p_roas, '$.value') AS value_purchase_roas,
    JSON_EXTRACT_SCALAR(web_purchase_roas, '$.1d_click') AS web_purchase_roas_1d_click,
    JSON_EXTRACT_SCALAR(web_purchase_roas, '$.1d_view') AS web_purchase_roas_1d_view,
    JSON_EXTRACT_SCALAR(web_purchase_roas, '$.28d_click') AS web_purchase_roas_28d_click,
    JSON_EXTRACT_SCALAR(web_purchase_roas, '$.28d_view') AS web_purchase_roas_28d_view,
    JSON_EXTRACT_SCALAR(web_purchase_roas, '$.7d_click') AS web_purchase_roas_7d_click,
    JSON_EXTRACT_SCALAR(web_purchase_roas, '$.7d_view') AS web_purchase_roas_7d_view,
    JSON_EXTRACT_SCALAR(web_purchase_roas, '$.action_type') AS web_purchase_roas_action_type,
    JSON_EXTRACT_SCALAR(web_purchase_roas, '$.value') AS web_purchase_roas_value
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY adset_id
        ORDER BY _airbyte_extracted_at DESC
      ) AS row_num
    FROM
      shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
      UNNEST(JSON_EXTRACT_ARRAY(mobile_app_purchase_roas)) AS roas,
      UNNEST(JSON_EXTRACT_ARRAY(purchase_roas)) AS p_roas,
      UNNEST(JSON_EXTRACT_ARRAY(website_purchase_roas)) AS web_purchase_roas
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1
) AS SOURCE
ON TARGET.adset_id = SOURCE.adset_id
WHEN MATCHED AND TARGET._airbyte_extracted_at > SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.adset_id = SOURCE.adset_id,
  TARGET.ad_id = SOURCE.ad_id,
  TARGET.one_day_click_roas = SOURCE.one_day_click_roas,
  TARGET.one_day_view_roas = SOURCE.one_day_view_roas,
  TARGET.twentyeight_day_click_roas = SOURCE.twentyeight_day_click_roas,
  TARGET.twentyeight_day_view_roas = SOURCE.twentyeight_day_view_roas,
  TARGET.seven_day_click_roas = SOURCE.seven_day_click_roas,
  TARGET.seven_day_view_roas = SOURCE.seven_day_view_roas,
  TARGET.roas_action_type = SOURCE.roas_action_type,
  TARGET.value_roas = SOURCE.value_roas,
  TARGET.one_day_click_purchase_roas = SOURCE.one_day_click_purchase_roas,
  TARGET.one_day_view_purchase_roas = SOURCE.one_day_view_purchase_roas,
  TARGET.twentyeight_day_click_purchase_roas = SOURCE.twentyeight_day_click_purchase_roas,
  TARGET.twentyeight_day_view_purchase_roas = SOURCE.twentyeight_day_view_purchase_roas,
  TARGET.seven_day_click_purchase_roas = SOURCE.seven_day_click_purchase_roas,
  TARGET.seven_day_view_purchase_roas = SOURCE.seven_day_view_purchase_roas,
  TARGET.purchase_roas_action_type = SOURCE.purchase_roas_action_type,
  TARGET.value_purchase_roas = SOURCE.value_purchase_roas,
  TARGET.web_purchase_roas_1d_click = SOURCE.web_purchase_roas_1d_click,
  TARGET.web_purchase_roas_1d_view = SOURCE.web_purchase_roas_1d_view,
  TARGET.web_purchase_roas_28d_click = SOURCE.web_purchase_roas_28d_click,
  TARGET.web_purchase_roas_28d_view = SOURCE.web_purchase_roas_28d_view,
  TARGET.web_purchase_roas_7d_click = SOURCE.web_purchase_roas_7d_click,
  TARGET.web_purchase_roas_7d_view = SOURCE.web_purchase_roas_7d_view,
  TARGET.web_purchase_roas_action_type = SOURCE.web_purchase_roas_action_type,
  TARGET.web_purchase_roas_value = SOURCE.web_purchase_roas_value
WHEN NOT MATCHED
THEN INSERT (
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  one_day_click_roas,
  one_day_view_roas,
  twentyeight_day_click_roas,
  twentyeight_day_view_roas,
  seven_day_click_roas,
  seven_day_view_roas,
  roas_action_type,
  value_roas,
  one_day_click_purchase_roas,
  one_day_view_purchase_roas,
  twentyeight_day_click_purchase_roas,
  twentyeight_day_view_purchase_roas,
  seven_day_click_purchase_roas,
  seven_day_view_purchase_roas,
  purchase_roas_action_type,
  value_purchase_roas,
  web_purchase_roas_1d_click,
  web_purchase_roas_1d_view,
  web_purchase_roas_28d_click,
  web_purchase_roas_28d_view,
  web_purchase_roas_7d_click,
  web_purchase_roas_7d_view,
  web_purchase_roas_action_type,
  web_purchase_roas_value
)
VALUES (
  SOURCE._airbyte_extracted_at,
  SOURCE.campaign_id,
  SOURCE.adset_id,
  SOURCE.ad_id,
  SOURCE.one_day_click_roas,
  SOURCE.one_day_view_roas,
  SOURCE.twentyeight_day_click_roas,
  SOURCE.twentyeight_day_view_roas,
  SOURCE.seven_day_click_roas,
  SOURCE.seven_day_view_roas,
  SOURCE.roas_action_type,
  SOURCE.value_roas,
  SOURCE.one_day_click_purchase_roas,
  SOURCE.one_day_view_purchase_roas,
  SOURCE.twentyeight_day_click_purchase_roas,
  SOURCE.twentyeight_day_view_purchase_roas,
  SOURCE.seven_day_click_purchase_roas,
  SOURCE.seven_day_view_purchase_roas,
  SOURCE.purchase_roas_action_type,
  SOURCE.value_purchase_roas,
  SOURCE.web_purchase_roas_1d_click,
  SOURCE.web_purchase_roas_1d_view,
  SOURCE.web_purchase_roas_28d_click,
  SOURCE.web_purchase_roas_28d_view,
  SOURCE.web_purchase_roas_7d_click,
  SOURCE.web_purchase_roas_7d_view,
  SOURCE.web_purchase_roas_action_type,
  SOURCE.web_purchase_roas_value
);
