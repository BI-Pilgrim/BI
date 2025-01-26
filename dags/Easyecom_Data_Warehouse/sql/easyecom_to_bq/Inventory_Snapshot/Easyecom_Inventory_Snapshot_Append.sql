MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Snapshot` AS TARGET
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
ROW_NUMBER() OVER(PARTITiON BY id ORDER BY ee_extracted_at DESC) as row_num
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
WHEN NOT MATCHED
THEN INSERT
(
  id,
  c_id,
  companyname,
  job_type_id,
  entry_date,
  file_url
)
VALUES
(
SOURCE.id,
SOURCE.c_id,
SOURCE.companyname,
SOURCE.job_type_id,
SOURCE.entry_date,
SOURCE.file_url
)
