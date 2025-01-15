MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.clicks` AS TARGET
USING (
  SELECT
    _airbyte_extracted_at,
    campaign_id,
    adset_id,
    ad_id,
    JSON_EXTRACT_SCALAR(unique_outbound, '$.action_target_id') AS unique_outbound_target_id,
    JSON_EXTRACT_SCALAR(unique_outbound, '$.value') AS unique_outbound_clicks,
    JSON_EXTRACT_SCALAR(value, '$.action_destination') AS action_destination,
    JSON_EXTRACT_SCALAR(value, '$.action_target_id') AS action_target_id,
    JSON_EXTRACT_SCALAR(value, '$.action_type') AS action_type,
    JSON_EXTRACT_SCALAR(value, '$.value') AS click_value
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY adset_id
        ORDER BY _airbyte_extracted_at DESC
      ) AS row_num
    FROM
      shopify-pubsub-project.pilgrim_bi_airbyte_facebook.ads_insights,
      UNNEST(JSON_EXTRACT_ARRAY(outbound_clicks, '$')) AS value,
      UNNEST(JSON_EXTRACT_ARRAY(unique_outbound_clicks)) AS unique_outbound
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1
) AS SOURCE
ON TARGET.adset_id = SOURCE.adset_id
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.adset_id = SOURCE.adset_id,
  TARGET.ad_id = SOURCE.ad_id,
  TARGET.unique_outbound_target_id = SOURCE.unique_outbound_target_id,
  TARGET.unique_outbound_clicks = SOURCE.unique_outbound_clicks,
  TARGET.action_destination = SOURCE.action_destination,
  TARGET.action_target_id = SOURCE.action_target_id,
  TARGET.action_type = SOURCE.action_type,
  TARGET.click_value = SOURCE.click_value
WHEN NOT MATCHED
THEN INSERT (
  _airbyte_extracted_at,
  campaign_id,
  adset_id,
  ad_id,
  unique_outbound_target_id,
  unique_outbound_clicks,
  action_destination,
  action_target_id,
  action_type,
  click_value
)
VALUES (
  SOURCE._airbyte_extracted_at,
  SOURCE.campaign_id,
  SOURCE.adset_id,
  SOURCE.ad_id,
  SOURCE.unique_outbound_target_id,
  SOURCE.unique_outbound_clicks,
  SOURCE.action_destination,
  SOURCE.action_target_id,
  SOURCE.action_type,
  SOURCE.click_value
);
