MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace` AS TARGET
USING
(
SELECT
  CAST(marketplace_id AS STRING) AS marketplace_id,
  CAST(name AS STRING) AS name,
  ee_extracted_at,
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY ee_extracted_at ORDER BY ee_extracted_at DESC) as row_num
FROM `shopify-pubsub-project.easycom.marketplace`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE.marketplace_id = TARGET.marketplace_id
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
THEN UPDATE SET
TARGET.marketplace_id = SOURCE.marketplace_id,
TARGET.name = SOURCE.name,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
  marketplace_id,
  name,
  ee_extracted_at
)
VALUES
(
SOURCE.marketplace_id,
SOURCE.name,
SOURCE.ee_extracted_at
)
