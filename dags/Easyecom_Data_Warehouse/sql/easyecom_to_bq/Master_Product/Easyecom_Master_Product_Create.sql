CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Master_products`
PARTITION BY DATE_TRUNC(created_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Master Product table is partitioned on created_at date at day level",
 require_partition_filter = False
 )
 AS


select
  SAFE_CAST(cp_id AS STRING) AS cp_id, 
  SAFE_CAST(product_id AS STRING) AS product_id, 
  SAFE_CAST(sku AS STRING) AS sku, 
  SAFE_CAST(product_name AS STRING) AS product_name, 
  description,
  active,
  created_at,
  updated_at,
  inventory,
  SAFE_CAST(product_type AS STRING) AS product_type,
  SAFE_CAST(brand AS STRING) AS brand,
  SAFE_CAST(colour AS STRING) AS colour,
  SAFE_CAST(category_id AS STRING) AS category_id,
  SAFE_CAST(brand_id AS STRING) AS brand_id,
  SAFE_CAST(accounting_sku AS STRING) AS accounting_sku,
  SAFE_CAST(accounting_unit AS STRING) AS accounting_unit,
  SAFE_CAST(category_name AS STRING) AS category_name,
  expiry_type,
  SAFE_CAST(company_name AS STRING) AS company_name,
  SAFE_CAST(c_id AS STRING) AS c_id,
  height,
  length,
  width,
  weight,
  cost,
  mrp,
  cp_sub_products_count,
  SAFE_CAST(size AS STRING) AS size,
  SAFE_CAST(model_no AS STRING) AS model_no,
  SAFE_CAST(hsn_code AS STRING) AS hsn_code,
  SAFE_CAST(product_shelf_life AS STRING) AS product_shelf_life,
  SAFE_CAST(product_image_url AS STRING) AS product_image_url,
  cp_inventory,
  custom_fields,
  sub_products,
  tax_rate,
  ee_extracted_at
  FROM `shopify-pubsub-project.easycom.master_products`

