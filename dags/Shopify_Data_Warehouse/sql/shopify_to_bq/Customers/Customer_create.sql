

CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customers`
PARTITION BY DATE_TRUNC(Customer_created_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Customer table is partitioned on customer created at day level",
 require_partition_filter = False
 )
 AS

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
  admin_graphql_api_id as admin_graphql_api_id,

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

  FROM `shopify-pubsub-project.pilgrim_bi_airbyte.customers`
