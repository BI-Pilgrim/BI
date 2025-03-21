MERGE INTO `shopify-pubsub-project.Shopify_Production.Customer_Master` AS target

  USING (
    
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
      MC.metafield_owner_resource,

      MIN(EXTRACT(DATE FROM O.Order_created_at AT TIME ZONE "Asia/Kolkata")) AS first_order_date,  
      MAX(EXTRACT(DATE FROM O.Order_created_at AT TIME ZONE "Asia/Kolkata")) AS last_order_date,
      COUNT(DISTINCT O.Order_name) AS order_count,
      SUM(O.Order_total_price) AS ltv,
      CASE 
        WHEN COUNT(DISTINCT O.Order_name) = 1 THEN 'New'
        ELSE 'Old'
      END AS customer_type

    FROM
      `shopify-pubsub-project.Shopify_staging.Customers` AS C
      LEFT OUTER JOIN `shopify-pubsub-project.Shopify_staging.Metafield_customers` AS MC 
      ON C.Customer_id = MC.customer_id
      LEFT JOIN `shopify-pubsub-project.Shopify_Production.Order_Master` as O
      ON C.Customer_id = O.customer_id

   
      WHERE 
        date(C._airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
        AND O.Order_cancelled_reason IS NULL         
        AND O.isTestOrder = FALSE                
        AND O.Order_fulfillment_status = 'fulfilled' 

      GROUP BY
      C._airbyte_extracted_at,C.Customer_accepts_marketing,C.Customer_tax_exempt,C.Customer_verified_email,
      C.Customer_id,C.Customer_phone,C.Customer_last_name,
      C.Customer_first_name, C.Customer_marketing_opt_in_level,
      C.Customer_email, C.Customer_state,C.Customer_tags,C.Customer_updated_at,
      C.Customer_created_at,C.Customer_accepts_marketing_updated_at,
      C.Customer_address1,C.Customer_address2,
      C.Customer_city,C.Customer_country_code,C.Customer_country_name,
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
      MC._airbyte_extracted_at,
      MC.Gender_field,
      MC.Personalization_field,
      MC.Concerns_field,
      MC.customer_id,
      MC.metafield_shop_url,
      MC.customer_created_at ,
      MC.customer_updated_at,
      MC.metafield_owner_resource
   
   ) AS source
  ON target.Customer_id = source.Customer_id

  WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
  target._airbyte_extracted_at = source._airbyte_extracted_at,
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
  target.email_consent_state = source.email_consent_state,
  target.MC_airbyte_extracted_at = source.MC_airbyte_extracted_at,
  target.Gender_field = source.Gender_field,
  target.Personalization_field = source.Personalization_field,
  target.Concerns_field = source.Concerns_field,
  target.MC_customer_id = source.MC_customer_id,
  target.metafield_shop_url = source.metafield_shop_url,
  target.MC_customer_created_at = source.MC_customer_created_at,
  target.MC_customer_updated_at = source.MC_customer_updated_at,
  target.metafield_owner_resource = source.metafield_owner_resource,

  target.last_order_date = source.last_order_date,
  target.order_count = target.order_count + source.order_count,
  target.ltv = target.ltv + source.ltv,
  target.customer_type = source.customer_type


  WHEN NOT MATCHED THEN INSERT (
  _airbyte_extracted_at,
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
  email_consent_state,
  MC_airbyte_extracted_at,
  Gender_field,
  Personalization_field,
  Concerns_field,
  MC_customer_id,
  metafield_shop_url,
  MC_customer_created_at,
  MC_customer_updated_at,
  metafield_owner_resource,
  first_order_date,
  last_order_date,
  order_count,
  ltv,
  customer_type



    )
    VALUES (
  source._airbyte_extracted_at,
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
  source.email_consent_state,
  source.MC_airbyte_extracted_at,
  source.Gender_field,
  source.Personalization_field,
  source.Concerns_field,
  source.MC_customer_id,
  source.metafield_shop_url,
  source.MC_customer_created_at,
  source.MC_customer_updated_at,
  source.metafield_owner_resource,
  source.first_order_date,
  source.last_order_date,
  source.order_count,
  source.ltv,
  source.customer_type

    )
