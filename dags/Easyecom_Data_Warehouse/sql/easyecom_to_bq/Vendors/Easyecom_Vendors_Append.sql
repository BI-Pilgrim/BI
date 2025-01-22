MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Vendors` AS TARGET
USING
(
select
  CAST(vendor_name AS STRING) AS vendor_name,
  CAST(vendor_c_id AS STRING) AS vendor_c_id,
  CAST(api_token AS STRING) AS api_token,
  CAST(dispatch_address AS STRING) AS dispatch_address,
  CAST(dispatch_city AS STRING) AS dispatch_city,
  CAST(dispatch_state_id AS STRING) AS dispatch_state_id,
  CAST(dispatch_state_name AS STRING) AS dispatch_state_name,
  CAST(dispatch_zip AS STRING) AS dispatch_zip,
  CAST(dispatch_country AS STRING) AS dispatch_country,
  CAST(billing_address AS STRING) AS billing_address,
  CAST(billing_city AS STRING) AS billing_city,
  CAST(billing_state_id AS STRING) AS billing_state_id,
  CAST(billing_state_name AS STRING) AS billing_state_name,
  CAST(billing_zip AS STRING) AS billing_zip,
  CAST(billing_country AS STRING) AS billing_country,
  CAST(dl_number AS STRING) AS dl_number,
  CAST(dl_expiry AS STRING) AS dl_expiry,
  CAST(fssai_number AS STRING) AS fssai_number,
  CAST(fssai_expiry AS STRING) AS fssai_expiry,
  ee_extracted_at
FROM
(
SELECT
*,
  
  ROW_NUMBER() OVER(PARTITION BY ee_extracted_at ORDER BY ee_extracted_at DESC) AS row_num
FROM `shopify-pubsub-project.easycom.vendors`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
) AS SOURCE
ON TARGET.vendor_c_id = SOURCE.vendor_c_id
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
THEN UPDATE SET
TARGET.vendor_name = SOURCE.vendor_name,
TARGET.vendor_c_id = SOURCE.vendor_c_id,
TARGET.api_token = SOURCE.api_token,
TARGET.dispatch_address = SOURCE.dispatch_address,
TARGET.dispatch_city = SOURCE.dispatch_city,
TARGET.dispatch_state_id = SOURCE.dispatch_state_id,
TARGET.dispatch_state_name = SOURCE.dispatch_state_name,
TARGET.dispatch_zip = SOURCE.dispatch_zip,
TARGET.dispatch_country = SOURCE.dispatch_country,
TARGET.billing_address = SOURCE.billing_address,
TARGET.billing_city = SOURCE.billing_city,
TARGET.billing_state_id = SOURCE.billing_state_id,
TARGET.billing_state_name = SOURCE.billing_state_name,
TARGET.billing_zip = SOURCE.billing_zip,
TARGET.billing_country = SOURCE.billing_country,
TARGET.dl_number = SOURCE.dl_number,
TARGET.dl_expiry = SOURCE.dl_expiry,
TARGET.fssai_number = SOURCE.fssai_number,
TARGET.fssai_expiry = SOURCE.fssai_expiry,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
  vendor_name,
  vendor_c_id,
  api_token,
  dispatch_address,
  dispatch_city,
  dispatch_state_id,
  dispatch_state_name,
  dispatch_zip,
  dispatch_country,
  billing_address,
  billing_city,
  billing_state_id,
  billing_state_name,
  billing_zip,
  billing_country,
  dl_number,
  dl_expiry,
  fssai_number,
  fssai_expiry,
  ee_extracted_at
)
VALUES
(
SOURCE.vendor_name,
SOURCE.vendor_c_id,
SOURCE.api_token,
SOURCE.dispatch_address,
SOURCE.dispatch_city,
SOURCE.dispatch_state_id,
SOURCE.dispatch_state_name,
SOURCE.dispatch_zip,
SOURCE.dispatch_country,
SOURCE.billing_address,
SOURCE.billing_city,
SOURCE.billing_state_id,
SOURCE.billing_state_name,
SOURCE.billing_zip,
SOURCE.billing_country,
SOURCE.dl_number,
SOURCE.dl_expiry,
SOURCE.fssai_number,
SOURCE.fssai_expiry,
SOURCE.ee_extracted_at
)