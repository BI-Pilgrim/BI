MERGE INTO `shopify-pubsub-project.Data_Warehouse_GoogleAds_Staging.ad_group_label` AS TARGET
USING
(
  SELECT
    _airbyte_extracted_at,
    ad_group_id,
    ad_group_label_resource_name,
    ad_group_resource_name,
    label_id,
    label_name,
    label_resource_name,
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY label_id ORDER BY _airbyte_extracted_at DESC) AS row_num
    FROM
      `shopify-pubsub-project.pilgrim_bi_google_ads.ad_group_label`
    WHERE
      DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1 -- Keep only the most recent row per label_id
) AS SOURCE
ON
  TARGET.label_id = SOURCE.label_id
WHEN
  MATCHED AND SOURCE._airbyte_extracted_at > TARGET._airbyte_extracted_at
THEN
  UPDATE SET
    TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
    TARGET.ad_group_id = SOURCE.ad_group_id,
    TARGET.ad_group_label_resource_name = SOURCE.ad_group_label_resource_name,
    TARGET.ad_group_resource_name = SOURCE.ad_group_resource_name,
    TARGET.label_id = SOURCE.label_id,
    TARGET.label_name = SOURCE.label_name,
    TARGET.label_resource_name = SOURCE.label_resource_name
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  ad_group_id,
  ad_group_label_resource_name,
  ad_group_resource_name,
  label_id,
  label_name,
  label_resource_name
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.ad_group_id,
  SOURCE.ad_group_label_resource_name,
  SOURCE.ad_group_resource_name,
  SOURCE.label_id,
  SOURCE.label_name,
  SOURCE.label_resource_name
);
