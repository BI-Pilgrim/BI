<<<<<<< Updated upstream
MERGE INTO `` AS TARGET
=======
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Master_products` AS TARGET
>>>>>>> Stashed changes
USING
(
SELECT
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
FROM
(
SELECT
*,
<<<<<<< Updated upstream
ROW_NUMBER() OVER(PARTITON BY ORDER BY _airbyte_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.master_products`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE. = TARGET.
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
THEN UPDATE SET
TARGET.cp_id = SOURCE.cp_id,
TARGET.product_id = SOURCE.product_id,
TARGET.SAFE_CAST(sku AS STRING) AS sku = SOURCE.SAFE_CAST(sku AS STRING) AS sku,
=======
ROW_NUMBER() OVER(PARTITION BY ee_extracted_at ORDER BY ee_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.master_products`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE.cp_id = TARGET.cp_id
WHEN MATCHED AND TARGET.updated_at < SOURCE.updated_at
THEN UPDATE SET
TARGET.cp_id = SOURCE.cp_id,
TARGET.product_id = SOURCE.product_id,
TARGET.sku = SOURCE.sku,
>>>>>>> Stashed changes
TARGET.product_name = SOURCE.product_name,
TARGET.description = SOURCE.description,
TARGET.active = SOURCE.active,
TARGET.created_at = SOURCE.created_at,
TARGET.updated_at = SOURCE.updated_at,
TARGET.inventory = SOURCE.inventory,
TARGET.product_type = SOURCE.product_type,
TARGET.brand = SOURCE.brand,
TARGET.colour = SOURCE.colour,
TARGET.category_id = SOURCE.category_id,
TARGET.brand_id = SOURCE.brand_id,
TARGET.accounting_sku = SOURCE.accounting_sku,
TARGET.accounting_unit = SOURCE.accounting_unit,
TARGET.category_name = SOURCE.category_name,
TARGET.expiry_type = SOURCE.expiry_type,
TARGET.company_name = SOURCE.company_name,
TARGET.c_id = SOURCE.c_id,
TARGET.height = SOURCE.height,
TARGET.length = SOURCE.length,
TARGET.width = SOURCE.width,
TARGET.weight = SOURCE.weight,
TARGET.cost = SOURCE.cost,
TARGET.mrp = SOURCE.mrp,
TARGET.cp_sub_products_count = SOURCE.cp_sub_products_count,
TARGET.size = SOURCE.size,
TARGET.model_no = SOURCE.model_no,
TARGET.hsn_code = SOURCE.hsn_code,
TARGET.product_shelf_life = SOURCE.product_shelf_life,
TARGET.product_image_url = SOURCE.product_image_url,
TARGET.cp_inventory = SOURCE.cp_inventory,
TARGET.custom_fields = SOURCE.custom_fields,
TARGET.sub_products = SOURCE.sub_products,
TARGET.tax_rate = SOURCE.tax_rate,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
  cp_id, 
  product_id, 
<<<<<<< Updated upstream
  SAFE_CAST(sku AS STRING) AS sku, 
=======
  sku, 
>>>>>>> Stashed changes
  product_name, 
  description,
  active,
  created_at,
  updated_at,
  inventory,
  product_type,
  brand,
  colour,
  category_id,
  brand_id,
  accounting_sku,
  accounting_unit,
  category_name,
  expiry_type,
  company_name,
  c_id,
  height,
  length,
  width,
  weight,
  cost,
  mrp,
  cp_sub_products_count,
  size,
  model_no,
  hsn_code,
  product_shelf_life,
  product_image_url,
  cp_inventory,
  custom_fields,
  sub_products,
  tax_rate,
  ee_extracted_at
)
VALUES
(
SOURCE.cp_id,
SOURCE.product_id,
<<<<<<< Updated upstream
SOURCE.SAFE_CAST(sku AS STRING) AS sku,
=======
sku,
>>>>>>> Stashed changes
SOURCE.product_name,
SOURCE.description,
SOURCE.active,
SOURCE.created_at,
SOURCE.updated_at,
SOURCE.inventory,
SOURCE.product_type,
SOURCE.brand,
SOURCE.colour,
SOURCE.category_id,
SOURCE.brand_id,
SOURCE.accounting_sku,
SOURCE.accounting_unit,
SOURCE.category_name,
SOURCE.expiry_type,
SOURCE.company_name,
SOURCE.c_id,
SOURCE.height,
SOURCE.length,
SOURCE.width,
SOURCE.weight,
SOURCE.cost,
SOURCE.mrp,
SOURCE.cp_sub_products_count,
SOURCE.size,
SOURCE.model_no,
SOURCE.hsn_code,
SOURCE.product_shelf_life,
SOURCE.product_image_url,
SOURCE.cp_inventory,
SOURCE.custom_fields,
SOURCE.sub_products,
SOURCE.tax_rate,
SOURCE.ee_extracted_at
)