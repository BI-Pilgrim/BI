<<<<<<< Updated upstream
MERGE INTO `` AS TARGET
=======
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Snapshot` AS TARGET
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
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
=======
ROW_NUMBER() OVER(PARTITiON BY ee_extracted_at ORDER BY ee_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.inventory_snapshot`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE.id = TARGET.id
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
THEN UPDATE SET
TARGET.id = SOURCE.id,
TARGET.c_id = SOURCE.c_id,
TARGET.companyname = SOURCE.companyname,
TARGET.job_type_id = SOURCE.job_type_id,
TARGET.entry_date = SOURCE.entry_date,
TARGET.file_url = SOURCE.file_url
>>>>>>> Stashed changes
WHEN NOT MATCHED
THEN INSERT
(
  id,
<<<<<<< Updated upstream
  CAST(c_id AS STRING) AS c_id,
  companyname,
  CAST(job_type_id AS STRING) AS job_type_id,
  entry_date,
  file_url,
  ee_extracted_at
=======
  c_id,
  companyname,
  job_type_id,
  entry_date,
  file_url
>>>>>>> Stashed changes
)
VALUES
(
SOURCE.id,
<<<<<<< Updated upstream
SOURCE.CAST(c_id AS STRING) AS c_id,
SOURCE.companyname,
SOURCE.CAST(job_type_id AS STRING) AS job_type_id,
SOURCE.entry_date,
SOURCE.file_url,
SOURCE.ee_extracted_at
=======
SOURCE.c_id,
SOURCE.companyname,
SOURCE.job_type_id,
SOURCE.entry_date,
SOURCE.file_url
>>>>>>> Stashed changes
)
