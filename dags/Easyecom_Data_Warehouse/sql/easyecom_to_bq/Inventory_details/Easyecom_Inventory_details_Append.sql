<<<<<<< Updated upstream
MERGE INTO `` AS TARGET
=======
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_details` AS TARGET
>>>>>>> Stashed changes
USING
(
SELECT
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
FROM
(
SELECT
*,
<<<<<<< Updated upstream
ROW_NUMBER() OVER(PARTITON BY ORDER BY _airbyte_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.inventory_details`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE. = TARGET.
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
=======
ROW_NUMBER() OVER(PARTITiON BY ee_extracted_at ORDER BY ee_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.inventory_details`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE.company_product_id = TARGET.company_product_id
WHEN MATCHED AND TARGET.last_update_date < SOURCE.last_update_date
>>>>>>> Stashed changes
THEN UPDATE SET
TARGET.company_name = SOURCE.company_name,
TARGET.location_key = SOURCE.location_key,
TARGET.company_product_id = SOURCE.company_product_id,
TARGET.product_id = SOURCE.product_id,
TARGET.available_inventory = SOURCE.available_inventory,
TARGET.virtual_inventory_count = SOURCE.virtual_inventory_count,
TARGET.sku = SOURCE.sku,
TARGET.accounting_sku = SOURCE.accounting_sku,
TARGET.accounting_unit = SOURCE.accounting_unit,
TARGET.mrp = SOURCE.mrp,
TARGET.creation_date = SOURCE.creation_date,
TARGET.last_update_date = SOURCE.last_update_date,
TARGET.cost = SOURCE.cost,
TARGET.sku_tax_rate = SOURCE.sku_tax_rate,
TARGET.color = SOURCE.color,
TARGET.size = SOURCE.size,
TARGET.weight = SOURCE.weight,
TARGET.height = SOURCE.height,
TARGET.length = SOURCE.length,
TARGET.width = SOURCE.width,
TARGET.selling_price_threshold = SOURCE.selling_price_threshold,
TARGET.inventory_threshold = SOURCE.inventory_threshold,
TARGET.category = SOURCE.category,
TARGET.image_url = SOURCE.image_url,
TARGET.brand = SOURCE.brand,
TARGET.product_name = SOURCE.product_name,
TARGET.model_no = SOURCE.model_no,
TARGET.product_unique_code = SOURCE.product_unique_code,
TARGET.description = SOURCE.description,
<<<<<<< Updated upstream
TARGET.is_combo = SOURCE.is_combo,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
=======
TARGET.is_combo = SOURCE.is_combo
>>>>>>> Stashed changes
WHEN NOT MATCHED
THEN INSERT
(
  company_name,
  location_key,
  company_product_id,
  product_id,
  available_inventory,
  virtual_inventory_count,
  sku,
  accounting_sku,
  accounting_unit,
  mrp,
  creation_date,
  last_update_date,
  cost,
  sku_tax_rate,
  color,
  size,
  weight,
  height,
<<<<<<< Updated upstream
  length,
=======
  `length`,
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
  is_combo,
  ee_extracted_at
=======
  is_combo
>>>>>>> Stashed changes
)
VALUES
(
SOURCE.company_name,
SOURCE.location_key,
SOURCE.company_product_id,
SOURCE.product_id,
SOURCE.available_inventory,
SOURCE.virtual_inventory_count,
SOURCE.sku,
SOURCE.accounting_sku,
SOURCE.accounting_unit,
SOURCE.mrp,
SOURCE.creation_date,
SOURCE.last_update_date,
SOURCE.cost,
SOURCE.sku_tax_rate,
SOURCE.color,
SOURCE.size,
SOURCE.weight,
SOURCE.height,
SOURCE.length,
SOURCE.width,
SOURCE.selling_price_threshold,
SOURCE.inventory_threshold,
SOURCE.category,
SOURCE.image_url,
SOURCE.brand,
SOURCE.product_name,
SOURCE.model_no,
SOURCE.product_unique_code,
SOURCE.description,
<<<<<<< Updated upstream
SOURCE.is_combo,
SOURCE.ee_extracted_at
=======
SOURCE.is_combo
>>>>>>> Stashed changes
)