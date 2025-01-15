CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_details`
PARTITION BY DATE_TRUNC(creation_date,day)
-- CLUSTER BY 
OPTIONS(
 description = "Inventory Details table is partitioned on creation_date date at day level",
 require_partition_filter = False
 )
 AS


select
  company_name,
  location_key,
  SAFE_CAST(company_product_id AS STRING) AS company_product_id,
  SAFE_CAST(product_id AS STRING) AS product_id,
  available_inventory,
  virtual_inventory_count,
  sku,
  accounting_sku,
  SAFE_CAST(accounting_unit AS FLOAT64) AS accounting_unit,
  mrp,
  creation_date,
  last_update_date,
  cost,
  sku_tax_rate,
  color,
  size,
  weight,
  height,
  length,
  width,
  selling_price_threshold,
  inventory_threshold,
  category,
  image_url,
  brand,
  product_name,
  model_no,
  product_unique_code,
  description,
  is_combo,
  ee_extracted_at

  FROM `shopify-pubsub-project.easycom.inventory_details`

