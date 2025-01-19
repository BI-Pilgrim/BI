CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Vendors`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = " table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


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

  FROM `shopify-pubsub-project.easycom.vendors`