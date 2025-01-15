MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.States`` AS TARGET
USING
(
select
  state_id,
  name,
  is_union_territory,
  zip_start_range,
  zip_end_range,
  postal_code,
  country_id,
  Zone,
  ee_extracted_at,
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITTION BY  ORDER BY ) AS row_num
FROM `shopify-pubsub-project.easycom.states`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
) AS SOURCE
ON TARGET. = SOURCE.
WHEN MATCHED AND TARGET. > SOURCE.
THEN UPDATE SET
TARGET.state_id = SOURCE.state_id,
TARGET.name = SOURCE.name,
TARGET.is_union_territory = SOURCE.is_union_territory,
TARGET.zip_start_range = SOURCE.zip_start_range,
TARGET.zip_end_range = SOURCE.zip_end_range,
TARGET.postal_code = SOURCE.postal_code,
TARGET.country_id = SOURCE.country_id,
TARGET.Zone = SOURCE.Zone,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
  state_id,
  name,
  is_union_territory,
  zip_start_range,
  zip_end_range,
  postal_code,
  country_id,
  Zone,
  ee_extracted_at
)
VALUES
(
SOURCE.state_id,
SOURCE.name,
SOURCE.is_union_territory,
SOURCE.zip_start_range,
SOURCE.zip_end_range,
SOURCE.postal_code,
SOURCE.country_id,
SOURCE.Zone,
SOURCE.ee_extracted_at
)