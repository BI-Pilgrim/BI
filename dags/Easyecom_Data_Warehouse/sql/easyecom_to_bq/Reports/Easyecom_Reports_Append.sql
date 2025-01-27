MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Reports` AS TARGET
USING
(
select
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  status,
  csv_url,
  inventory_type,
  ee_extracted_at,
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY report_id ORDER BY created_on DESC) AS row_num
FROM `shopify-pubsub-project.easycom.reports`
WHERE DATE(end_date) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
) AS SOURCE
ON TARGET.report_id = SOURCE.report_id

WHEN MATCHED AND TARGET.created_on < SOURCE.created_on
THEN UPDATE SET
TARGET.report_type = SOURCE.report_type,
TARGET.start_date = SOURCE.start_date,
TARGET.end_date = SOURCE.end_date,
TARGET.created_on = SOURCE.created_on,
TARGET.status = SOURCE.status,
TARGET.csv_url = SOURCE.csv_url,
TARGET.inventory_type = SOURCE.inventory_type,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
  report_type,
  start_date,
  end_date,
  created_on,
  status,
  csv_url,
  inventory_type,
  ee_extracted_at
)
VALUES
(
SOURCE.report_type,
SOURCE.start_date,
SOURCE.end_date,
SOURCE.created_on,
SOURCE.status,
SOURCE.csv_url,
SOURCE.inventory_type,
SOURCE.ee_extracted_at
)