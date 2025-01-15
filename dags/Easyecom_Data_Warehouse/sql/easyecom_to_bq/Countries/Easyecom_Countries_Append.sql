<<<<<<< Updated upstream
MERGE INTO `` AS TARGET
=======
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Countries` AS TARGET
>>>>>>> Stashed changes
USING
(
SELECT
  country_id,
  country,
  default_currency_code,
  code_2,
  code_3,
  ee_extracted_at,
FROM
(
SELECT
*,
<<<<<<< Updated upstream
ROW_NUMBER() OVER(PARTITON BY ORDER BY _airbyte_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.countries`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE. = TARGET.
WHEN MATCHED AND TARGET._airbyte_extracted_at < SOURCE._airbyte_extracted_at
=======
ROW_NUMBER() OVER(PARTITiON BY ee_extracted_at ORDER BY ee_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.countries`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE.country_id = TARGET.country_id
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
>>>>>>> Stashed changes
THEN UPDATE SET
TARGET.country_id = SOURCE.country_id,
TARGET.country = SOURCE.country,
TARGET.default_currency_code = SOURCE.default_currency_code,
TARGET.code_2 = SOURCE.code_2,
TARGET.code_3 = SOURCE.code_3,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
  country_id,
  country,
  default_currency_code,
  code_2,
  code_3,
  ee_extracted_at
)
VALUES
(
SOURCE.country_id,
SOURCE.country,
SOURCE.default_currency_code,
SOURCE.code_2,
SOURCE.code_3,
SOURCE.ee_extracted_at
)
