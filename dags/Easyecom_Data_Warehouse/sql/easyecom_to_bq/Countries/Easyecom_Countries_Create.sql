
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Countries`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Countries table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select
  country_id,
  country,
  default_currency_code,
  code_2,
  code_3,
  ee_extracted_at,
  FROM `shopify-pubsub-project.easycom.countries`