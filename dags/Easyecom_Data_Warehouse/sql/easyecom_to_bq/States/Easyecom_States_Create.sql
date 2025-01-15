
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.States`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "States table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select

  state_id,
  name,
  is_union_territory,
  zip_start_range,
  zip_end_range,
  postal_code,
  country_id,
  Zone,
  ee_extracted_at,

  FROM `shopify-pubsub-project.easycom.states`
  