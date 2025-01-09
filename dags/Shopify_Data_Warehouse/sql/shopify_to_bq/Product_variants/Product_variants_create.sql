
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Product_variants`
PARTITION BY DATE_TRUNC(variant_created_at, DAY)
CLUSTER BY variant_id
OPTIONS(
  description = "Products variants table is partitioned on variant created at",
  require_partition_filter = FALSE
)
 AS 
SELECT  
distinct
_airbyte_extracted_at,
id as variant_id,
sku as variant_sku,
grams,
price,
title,
barcode,
taxable,
image_id,
position,
shop_url,
image_src,
image_url,
created_at as variant_created_at,
product_id,
updated_at as variant_updated_at,
weight_unit,
display_name,
compare_at_price,
inventory_policy,
inventory_item_id,
requires_shipping,
available_for_sale,
inventory_quantity,
fulfillment_service,
admin_graphql_api_id,
inventory_management,
old_inventory_quantity
FROM `shopify-pubsub-project.pilgrim_bi_airbyte.product_variants`
