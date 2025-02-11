CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Marketplace_listings`
PARTITION BY DATE_TRUNC(ee_extracted_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Marketplace_listings table is partitioned on ee_extracted_date date at day level",
 require_partition_filter = False
 )
 AS
select
  *
  from
  (
  select
  CAST(name AS STRING) AS name,
  CAST(sku AS STRING) AS sku,
  CAST(master_sku AS STRING) AS master_sku,
  CAST(mrp AS FLOAT64) AS mrp,
  CAST(site_uid AS STRING) AS site_uid,
  CAST(listing_ref_number AS STRING) AS listing_ref_number,
  CAST(uid AS STRING) AS uid,
  CAST(identifier AS STRING) AS identifier,
  CAST(title AS STRING) AS title,
  ee_extracted_at,
  row_number() over(partition by name,sku,master_sku,site_uid,listing_ref_number,uid,identifier,ee_extracted_at order by ee_extracted_at desc) as rn
  FROM `shopify-pubsub-project.easycom.marketplace_listings`
  )
  where rn = 1
