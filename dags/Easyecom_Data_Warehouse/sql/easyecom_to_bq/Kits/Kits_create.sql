CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Kits`
PARTITION BY DATE_TRUNC(add_date,day)
-- CLUSTER BY 
OPTIONS(
 description = "Kits table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select

  CAST(product_id AS STRING) AS product_id,
  CAST(sku AS STRING) AS sku,
  CAST(accounting_sku AS STRING) AS accounting_sku,
  CAST(accounting_unit AS STRING) AS accounting_unit,
  mrp,
  add_date,
  last_update_date,
  cost,
  CAST(hsn_code AS STRING) AS hsn_code,
  CAST(colour AS STRING) AS colour,
  weight,
  height,
  length,
  width,
  CAST(size AS STRING) AS size, 
  CAST(material_type AS STRING) AS material_type, 
  CAST(model_number AS STRING) AS model_number, 
  CAST(model_name AS STRING) AS model_name, 
  CAST(category AS STRING) AS category, 
  CAST(brand AS STRING) AS brand, 
  CAST(c_id AS STRING) AS c_id, 
  ee_extracted_at,
  FROM `shopify-pubsub-project.easycom.kits`