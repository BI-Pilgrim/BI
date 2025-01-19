
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Marketplace table is partitioned on ee_extracted_date date at day level",
 require_partition_filter = False
 )
 AS


select

  CAST(marketplace_id AS STRING) AS marketplace_id,
  CAST(name AS STRING) AS name,
  ee_extracted_at,

  FROM `shopify-pubsub-project.easycom.marketplace`