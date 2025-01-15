MERGE INTO `` AS TARGET
USING
(
SELECT
  id,
  CAST(c_id AS STRING) AS c_id,
  companyname,
  CAST(job_type_id AS STRING) AS job_type_id,
  entry_date,
  file_url,
  ee_extracted_at
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITON BY ORDER BY _airbyte_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.inventory_snapshot`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE. = TARGET.
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
THEN UPDATE SET
TARGET.id = SOURCE.id,
TARGET.CAST(c_id AS STRING) AS c_id = SOURCE.CAST(c_id AS STRING) AS c_id,
TARGET.companyname = SOURCE.companyname,
TARGET.CAST(job_type_id AS STRING) AS job_type_id = SOURCE.CAST(job_type_id AS STRING) AS job_type_id,
TARGET.entry_date = SOURCE.entry_date,
TARGET.file_url = SOURCE.file_url,
TARGET.ee_extracted_a = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
  id,
  CAST(c_id AS STRING) AS c_id,
  companyname,
  CAST(job_type_id AS STRING) AS job_type_id,
  entry_date,
  file_url,
  ee_extracted_at
)
VALUES
(
SOURCE.id,
SOURCE.CAST(c_id AS STRING) AS c_id,
SOURCE.companyname,
SOURCE.CAST(job_type_id AS STRING) AS job_type_id,
SOURCE.entry_date,
SOURCE.file_url,
SOURCE.ee_extracted_at
)
