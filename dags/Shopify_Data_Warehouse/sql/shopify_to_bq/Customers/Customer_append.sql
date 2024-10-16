
MERGE INTO `shopify-pubsub-project.Shopify_staging.Customers` AS target

USING (
  SELECT
  distinct
  _airbyte_extracted_at,
  accepts_marketing as Customer_accepts_marketing,
  tax_exempt as Customer_tax_exempt,
  verified_email as Customer_verified_email,
  CAST(id AS STRING) as Customer_id,
  phone as Customer_phone,
  last_name as Customer_last_name,
  first_name as Customer_first_name,
  marketing_opt_in_level as Customer_marketing_opt_in_level,
  email as Customer_email,
  state as Customer_state,
  tags as Customer_tags,
  updated_at as Customer_updated_at,
  created_at as Customer_created_at,
  CAST(JSON_EXTRACT_SCALAR(accepts_marketing_updated_at) AS TIMESTAMP) as Customer_accepts_marketing_updated_at,

  JSON_EXTRACT_SCALAR(default_address, '$.address1') AS Customer_address1,
  JSON_EXTRACT_SCALAR(default_address, '$.address2') AS Customer_address2,
  JSON_EXTRACT_SCALAR(default_address, '$.city') AS Customer_city,
  JSON_EXTRACT_SCALAR(default_address, '$.country_code') AS Customer_country_code,
  JSON_EXTRACT_SCALAR(default_address, '$.country_name') AS Customer_country_name,
  JSON_EXTRACT_SCALAR(default_address, '$.id') AS Address_id,
  JSON_EXTRACT_SCALAR(default_address, '$.province') AS Customer_province,
  JSON_EXTRACT_SCALAR(default_address, '$.province_code') AS Customer_province_code,
  JSON_EXTRACT_SCALAR(default_address, '$.zip') AS Customer_zip,

  JSON_EXTRACT_SCALAR(sms_marketing_consent, '$.consent_collected_from') AS sms_consent_collected_from,
  CAST(JSON_EXTRACT_SCALAR(sms_marketing_consent, '$.consent_updated_at') AS TIMESTAMP) AS sms_consent_updated_at,
  JSON_EXTRACT_SCALAR(sms_marketing_consent, '$.opt_in_level') AS sms_consent_opt_in_level,
  JSON_EXTRACT_SCALAR(sms_marketing_consent, '$.state') AS sms_consent_state,

  CAST(JSON_EXTRACT_SCALAR(email_marketing_consent, '$.consent_updated_at') AS TIMESTAMP) AS email_consent_updated_at,
  JSON_EXTRACT_SCALAR(email_marketing_consent, '$.opt_in_level') AS email_consent_opt_in_level,
  JSON_EXTRACT_SCALAR(email_marketing_consent, '$.state') AS email_consent_state,

  FROM `shopify-pubsub-project.airbyte711.customers`
    WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.Customer_id = source.Customer_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
target.Customer_accepts_marketing = source.Customer_accepts_marketing,
target.Customer_tax_exempt = source.Customer_tax_exempt,
target.Customer_verified_email = source.Customer_verified_email,
target.Customer_id = source.Customer_id,
target.Customer_phone = source.Customer_phone,
target.Customer_last_name = source.Customer_last_name,
target.Customer_first_name = source.Customer_first_name,
target.Customer_marketing_opt_in_level = source.Customer_marketing_opt_in_level,
target.Customer_email = source.Customer_email,
target.Customer_state = source.Customer_state,
target.Customer_tags = source.Customer_tags,
target.Customer_updated_at = source.Customer_updated_at,
target.Customer_created_at = source.Customer_created_at,
target.Customer_accepts_marketing_updated_at = source.Customer_accepts_marketing_updated_at,
target.Customer_address1 = source.Customer_address1,
target.Customer_address2 = source.Customer_address2,
target.Customer_city = source.Customer_city,
target.Customer_country_code = source.Customer_country_code,
target.Customer_country_name = source.Customer_country_name,
target.Address_id = source.Address_id,
target.Customer_province = source.Customer_province,
target.Customer_province_code = source.Customer_province_code,
target.Customer_zip = source.Customer_zip,
target.sms_consent_collected_from = source.sms_consent_collected_from,
target.sms_consent_updated_at = source.sms_consent_updated_at,
target.sms_consent_opt_in_level = source.sms_consent_opt_in_level,
target.sms_consent_state = source.sms_consent_state,
target.email_consent_updated_at = source.email_consent_updated_at,
target.email_consent_opt_in_level = source.email_consent_opt_in_level,
target.email_consent_state = source.email_consent_state

WHEN NOT MATCHED THEN INSERT (
Customer_accepts_marketing,
Customer_tax_exempt,
Customer_verified_email,
Customer_id,
Customer_phone,
Customer_last_name,
Customer_first_name,
Customer_marketing_opt_in_level,
Customer_email,
Customer_state,
Customer_tags,
Customer_updated_at,
Customer_created_at,
Customer_accepts_marketing_updated_at,
Customer_address1,
Customer_address2,
Customer_city,
Customer_country_code,
Customer_country_name,
Address_id,
Customer_province,
Customer_province_code,
Customer_zip,
sms_consent_collected_from,
sms_consent_updated_at,
sms_consent_opt_in_level,
sms_consent_state,
email_consent_updated_at,
email_consent_opt_in_level,
email_consent_state  
  )
  VALUES (

source.Customer_accepts_marketing,
source.Customer_tax_exempt,
source.Customer_verified_email,
source.Customer_id,
source.Customer_phone,
source.Customer_last_name,
source.Customer_first_name,
source.Customer_marketing_opt_in_level,
source.Customer_email,
source.Customer_state,
source.Customer_tags,
source.Customer_updated_at,
source.Customer_created_at,
source.Customer_accepts_marketing_updated_at,
source.Customer_address1,
source.Customer_address2,
source.Customer_city,
source.Customer_country_code,
source.Customer_country_name,
source.Address_id,
source.Customer_province,
source.Customer_province_code,
source.Customer_zip,
source.sms_consent_collected_from,
source.sms_consent_updated_at,
source.sms_consent_opt_in_level,
source.sms_consent_state,
source.email_consent_updated_at,
source.email_consent_opt_in_level,
source.email_consent_state
  )




