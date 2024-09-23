
CREATE or replace TABLE `shopify-pubsub-project.Shopify_Production.Customer_Master`
PARTITION BY DATE_TRUNC(Customer_created_at,day)
 
OPTIONS(
 description = "Customer Master table is partitioned on customer created at day level",
 require_partition_filter = False
 )
 AS

SELECT
    distinct
    C._airbyte_extracted_at,
    C.Customer_accepts_marketing,
    C.Customer_tax_exempt,
    C.Customer_verified_email,
    C.Customer_id,
    C.Customer_phone,
    C.Customer_last_name,
    C.Customer_first_name,
    C.Customer_marketing_opt_in_level,
    C.Customer_email,
    C.Customer_state,
    C.Customer_tags,
    C.Customer_updated_at,
    C.Customer_created_at,
    C.Customer_accepts_marketing_updated_at,
    C.Customer_address1,
    C.Customer_address2,
    C.Customer_city,
    C.Customer_country_code,
    C.Customer_country_name,
    C.Address_id,
    C.Customer_province,
    C.Customer_province_code,
    C.Customer_zip,
    C.sms_consent_collected_from,
    C.sms_consent_updated_at,
    C.sms_consent_opt_in_level,
    C.sms_consent_state,
    C.email_consent_updated_at,
    C.email_consent_opt_in_level,
    C.email_consent_state,
    MC._airbyte_extracted_at AS MC_airbyte_extracted_at,
    MC.Gender_field,
    MC.Personalization_field,
    MC.Concerns_field,
    MC.customer_id AS MC_customer_id,
    MC.metafield_shop_url,
    MC.customer_created_at AS MC_customer_created_at,
    MC.customer_updated_at AS MC_customer_updated_at,
    MC.metafield_owner_resource
  FROM
    `shopify-pubsub-project.Shopify_staging.Customers` AS C
    LEFT OUTER JOIN `shopify-pubsub-project.Shopify_staging.Metafield_customers` AS MC 
    ON C.Customer_id = MC.customer_id




