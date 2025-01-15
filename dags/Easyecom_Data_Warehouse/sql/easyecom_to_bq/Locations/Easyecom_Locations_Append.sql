MERGE INTO `` AS TARGET
USING
(
SELECT
  CAST(company_id AS STRING) AS company_id,
  CAST(location_key AS STRING) AS location_key,
  CAST(location_name AS STRING) AS location_name,
  CAST(is_store AS STRING) AS is_store,
  CAST(city AS STRING) AS city,
  CAST(state AS STRING) AS state,
  CAST(country AS STRING) AS country,
  CAST(zip AS STRING) AS zip,
  CAST(copy_master_from_primary AS STRING) AS copy_master_from_primary,
  CAST(address AS STRING) AS address,
  CAST(api_token AS STRING) AS api_token,
  CAST(user_id AS STRING) AS user_id,
  CAST(phone_number AS STRING) AS phone_number,
  CAST(billing_street AS STRING) AS billing_street,
  CAST(billing_state AS STRING) AS billing_state,
  CAST(billing_zipcode AS STRING) AS billing_zipcode,
  CAST(billing_country AS STRING) AS billing_country,
  CAST(pickup_street AS STRING) AS pickup_street,
  CAST(pickup_state AS STRING) AS pickup_state,
  CAST(pickup_zipcode AS STRING) AS pickup_zipcode,
  CAST(pickup_country AS STRING) AS pickup_country,
  CAST(stockHandle AS STRING) AS stockHandle,
  ee_extracted_at,
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
TARGET.company_id = SOURCE.company_id,
TARGET.location_key = SOURCE.location_key,
TARGET.location_name = SOURCE.location_name,
TARGET.is_store = SOURCE.is_store,
TARGET.city = SOURCE.city,
TARGET.state = SOURCE.state,
TARGET.country = SOURCE.country,
TARGET.zip = SOURCE.zip,
TARGET.copy_master_from_primary = SOURCE.copy_master_from_primary,
TARGET.address = SOURCE.address,
TARGET.api_token = SOURCE.api_token,
TARGET.user_id = SOURCE.user_id,
TARGET.phone_number = SOURCE.phone_number,
TARGET.billing_street = SOURCE.billing_street,
TARGET.billing_state = SOURCE.billing_state,
TARGET.billing_zipcode = SOURCE.billing_zipcode,
TARGET.billing_country = SOURCE.billing_country,
TARGET.pickup_street = SOURCE.pickup_street,
TARGET.pickup_state = SOURCE.pickup_state,
TARGET.pickup_zipcode = SOURCE.pickup_zipcode,
TARGET.pickup_country = SOURCE.pickup_country,
TARGET.stockHandle = SOURCE.stockHandle,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at,
WHEN NOT MATCHED
THEN INSERT
(
  company_id,
  location_key,
  location_name,
  is_store,
  city,
  state,
  country,
  zip,
  copy_master_from_primary,
  address,
  api_token,
  user_id,
  phone_number,
  billing_street,
  billing_state,
  billing_zipcode,
  billing_country,
  pickup_street,
  pickup_state,
  pickup_zipcode,
  pickup_country,
  stockHandle,
  ee_extracted_at,
)
VALUES
(
SOURCE.company_id,
SOURCE.location_key,
SOURCE.location_name,
SOURCE.is_store,
SOURCE.city,
SOURCE.state,
SOURCE.country,
SOURCE.zip,
SOURCE.copy_master_from_primary,
SOURCE.address,
SOURCE.api_token,
SOURCE.user_id,
SOURCE.phone_number,
SOURCE.billing_street,
SOURCE.billing_state,
SOURCE.billing_zipcode,
SOURCE.billing_country,
SOURCE.pickup_street,
SOURCE.pickup_state,
SOURCE.pickup_zipcode,
SOURCE.pickup_country,
SOURCE.stockHandle,
SOURCE.ee_extracted_at,
)
