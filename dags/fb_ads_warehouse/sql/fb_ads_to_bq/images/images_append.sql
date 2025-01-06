MERGE INTO `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.images` AS TARGET
USING
(
  SELECT
  _airbyte_extracted_at,
  id,
  url,
  name,
  width,
  height,
  status,
  url_128,
  account_id,
  created_time,
  updated_time,
  permalink_url,
  original_width,
  original_height,
  is_associated_creatives_in_adgroups,




  FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY id ORDER BY _airbyte_extracted_at) AS row_num
      FROM
        shopify-pubsub-project.pilgrim_bi_airbyte_facebook.images
      WHERE
        DATE(_airbyte_extracted_at) > DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
    )
  WHERE row_num = 1  
) AS SOURCE




ON TARGET.id = SOURCE.id
WHEN MATCHED AND TARGET._airbyte_extracted_at > SOURCE._airbyte_extracted_at
THEN UPDATE SET
  TARGET._airbyte_extracted_at = SOURCE._airbyte_extracted_at,
  TARGET.id = SOURCE.id,
  TARGET.url = SOURCE.url,
  TARGET.name = SOURCE.name,
  TARGET.width = SOURCE.width,
  TARGET.height = SOURCE.height,
  TARGET.status = SOURCE.status,
  TARGET.url_128 = SOURCE.url_128,
  TARGET.account_id = SOURCE.account_id,
  TARGET.created_time = SOURCE.created_time,
  TARGET.updated_time = SOURCE.updated_time,
  TARGET.permalink_url = SOURCE.permalink_url,
  TARGET.original_width = SOURCE.original_width,
  TARGET.original_height = SOURCE.original_height,
  TARGET.is_associated_creatives_in_adgroups = SOURCE.is_associated_creatives_in_adgroups
WHEN NOT MATCHED
THEN INSERT
(
  _airbyte_extracted_at,
  id,
  url,
  name,
  width,
  height,
  status,
  url_128,
  account_id,
  created_time,
  updated_time,
  permalink_url,
  original_width,
  original_height,
  is_associated_creatives_in_adgroups
)
VALUES
(
  SOURCE._airbyte_extracted_at,
  SOURCE.id,
  SOURCE.url,
  SOURCE.name,
  SOURCE.width,
  SOURCE.height,
  SOURCE.status,
  SOURCE.url_128,
  SOURCE.account_id,
  SOURCE.created_time,
  SOURCE.updated_time,
  SOURCE.permalink_url,
  SOURCE.original_width,
  SOURCE.original_height,
  SOURCE.is_associated_creatives_in_adgroups
)
