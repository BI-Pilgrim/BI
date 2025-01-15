
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Snapshot`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Inventory Snapshot table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select
  id,
  CAST(c_id AS STRING) AS c_id,
  companyname,
  CAST(job_type_id AS STRING) AS job_type_id,
  entry_date,
  file_url,
  ee_extracted_at

  FROM `shopify-pubsub-project.easycom.inventory_snapshot`