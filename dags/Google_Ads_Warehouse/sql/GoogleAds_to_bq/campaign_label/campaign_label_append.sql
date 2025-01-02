MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.campaign_label` as TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    label_id,
    label_name,
    campaign_id,
    label_resource_name,
    campaign_resource_name,
    campaign_label_resource_name,

  FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY campaign_id ORDER BY campaign_id DESC) AS ROW_NUM
    FROM
     `shopify-pubsub-project.pilgrim_bi_google_ads.campaign_label`
  )
  WHERE
    ROW_NUM = 1
) AS SOURCE
ON TARGET.campaign_id = SOURCE.campaign_id
WHEN MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.label_id = SOURCE.label_id,
  TARGET.label_name = SOURCE.label_name,
  TARGET.campaign_id = SOURCE.campaign_id,
  TARGET.label_resource_name = SOURCE.label_resource_name,
  TARGET.campaign_resource_name = SOURCE.campaign_resource_name,
  TARGET.campaign_label_resource_name = SOURCE.campaign_label_resource_name
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  label_id,
  label_name,
  campaign_id,
  label_resource_name,
  campaign_resource_name,
  campaign_label_resource_name
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.label_id,
  SOURCE.label_name,
  SOURCE.campaign_id,
  SOURCE.label_resource_name,
  SOURCE.campaign_resource_name,
  SOURCE.campaign_label_resource_name
)