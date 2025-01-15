CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Customers`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Customers table is partitioned on extracted_at date at day level",
 require_partition_filter = False
 )
 AS


select
  CAST(company_invoice_group_id AS STRING) AS company_invoice_group_id,
  CAST(c_id AS STRING) AS c_id,
  CAST(company_name AS STRING) AS company_name,
  CAST(pricing_group AS STRING) AS pricing_group,
  CAST(customer_support_email AS STRING) AS customer_support_email,
  CAST(customer_support_contact AS STRING) AS customer_support_contact,
  CAST(brand_description AS STRING) AS brand_description,
  CAST(currency_code AS STRING) AS currency_code,
  CAST(gst_num AS STRING) AS gst_num,
  CAST(type_of_customer AS STRING) AS type_of_customer,
  CAST(billing_street AS STRING) AS billing_street,
  CAST(billing_city AS STRING) AS billing_city,
  CAST(billing_zipcode AS STRING) AS billing_zipcode,
  CAST(billing_state AS STRING) AS billing_state,
  CAST(billing_country AS STRING) AS billing_country,
  CAST(dispatch_street AS STRING) AS dispatch_street,
  CAST(dispatch_city AS STRING) AS dispatch_city,
  CAST(dispatch_zipcode AS STRING) AS dispatch_zipcode,
  CAST(dispatch_state AS STRING) AS dispatch_state,
  CAST(dispatch_country AS STRING) AS dispatch_country,
  ee_extracted_at,

  FROM `shopify-pubsub-project.easycom.customers`