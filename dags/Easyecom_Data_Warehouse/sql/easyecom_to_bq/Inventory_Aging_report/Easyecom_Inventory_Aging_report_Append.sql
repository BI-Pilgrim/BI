MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Aging_report` AS TARGET
USING
(
SELECT
  Location,
  Company,
  sku,
  Name,
  EAN,
  HSN,
  Description,
  SAFE_CAST(D30 AS FLOAT64 ) AS D30,
  SAFE_CAST(D60 AS FLOAT64 ) AS D60,
  SAFE_CAST(D90 AS FLOAT64 ) AS D90,
  SAFE_CAST(D120 AS FLOAT64 ) AS D120,
  SAFE_CAST(D180 AS FLOAT64 ) AS D180,
  SAFE_CAST(D240 AS FLOAT64 ) AS D240,
  SAFE_CAST(D300 AS FLOAT64 ) AS D300,
  SAFE_CAST(D365 AS FLOAT64 ) AS D365,
  SAFE_CAST(D365Above AS FLOAT64 ) AS D365Above,
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  inventory_type,
  ee_extracted_at
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY ee_extracted_at ORDER BY ee_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.inventory_aging_report`
WHERE DATE(end_date) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE.sku = TARGET.sku AND SOURCE.EAN = TARGET.EAN
WHEN MATCHED AND TARGET.end_date < SOURCE.end_date
THEN UPDATE SET
TARGET.Location = SOURCE.Location,
TARGET.Company = SOURCE.Company,
TARGET.sku = SOURCE.sku,
TARGET.Name = SOURCE.Name,
TARGET.EAN = SOURCE.EAN,
TARGET.HSN = SOURCE.HSN,
TARGET.Description = SOURCE.Description,
TARGET.D30 = SOURCE.D30,
TARGET.D60 = SOURCE.D60,
TARGET.D90 = SOURCE.D90,
TARGET.D120 = SOURCE.D120,
TARGET.D180 = SOURCE.D180,
TARGET.D240 = SOURCE.D240,
TARGET.D300 = SOURCE.D300,
TARGET.D365 = SOURCE.D365,
TARGET.D365Above = SOURCE.D365Above,
TARGET.report_id = SOURCE.report_id,
TARGET.report_type = SOURCE.report_type,
TARGET.start_date = SOURCE.start_date,
TARGET.end_date = SOURCE.end_date,
TARGET.created_on = SOURCE.created_on,
TARGET.inventory_type = SOURCE.inventory_type
WHEN NOT MATCHED
THEN INSERT
(
  Location,
  Company,
  sku,
  Name,
  EAN,
  HSN,
  Description,
  D30,
  D60,
  D90,
  D120,
  D180,
  D240,
  D300,
  D365,
  D365Above,
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  inventory_type
)
VALUES
(
SOURCE.Location,
SOURCE.Company,
SOURCE.sku,
SOURCE.Name,
SOURCE.EAN,
SOURCE.HSN,
SOURCE.Description,
SOURCE.D30,
SOURCE.D60,
SOURCE.D90,
SOURCE.D120,
SOURCE.D180,
SOURCE.D240,
SOURCE.D300,
SOURCE.D365,
SOURCE.D365Above,
SOURCE.report_id,
SOURCE.report_type,
SOURCE.start_date,
SOURCE.end_date,
SOURCE.created_on,
SOURCE.inventory_type
)
