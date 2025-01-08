
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Product_variants` AS target  

USING (

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

WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source
ON target.variant_id = source.variant_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at

THEN UPDATE SET
target._airbyte_extracted_at = source._airbyte_extracted_at,

target.variant_id = source.variant_id,

target.variant_sku = source.variant_sku,

target.grams = source.grams,

target.price = source.price,

target.title = source.title,

target.barcode = source.barcode,

target.taxable = source.taxable,

target.image_id = source.image_id,

target.position = source.position,

target.shop_url = source.shop_url,

target.image_src = source.image_src,

target.image_url = source.image_url,

target.variant_created_at = source.variant_created_at,

target.product_id = source.product_id,

target.variant_updated_at = source.variant_updated_at,

target.weight_unit = source.weight_unit,

target.display_name = source.display_name,

target.compare_at_price = source.compare_at_price,

target.inventory_policy = source.inventory_policy,

target.inventory_item_id = source.inventory_item_id,

target.requires_shipping = source.requires_shipping,

target.available_for_sale = source.available_for_sale,

target.inventory_quantity = source.inventory_quantity,

target.fulfillment_service = source.fulfillment_service,

target.admin_graphql_api_id = source.admin_graphql_api_id,

target.inventory_management = source.inventory_management,

target.old_inventory_quantity = source.old_inventory_quantity

 

WHEN NOT MATCHED THEN INSERT (

_airbyte_extracted_at,

variant_id,

variant_sku,

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

variant_created_at,

product_id,

variant_updated_at,

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

   

  )

  VALUES (

  source._airbyte_extracted_at,

  source.variant_id,

  source.variant_sku,

  source.grams,

  source.price,

  source.title,

  source.barcode,

  source.taxable,

  source.image_id,

  source.position,

  source.shop_url,

  source.image_src,

  source.image_url,

  source.variant_created_at,

  source.product_id,

  source.variant_updated_at,

  source.weight_unit,

  source.display_name,

  source.compare_at_price,

  source.inventory_policy,

  source.inventory_item_id,

  source.requires_shipping,

  source.available_for_sale,

  source.inventory_quantity,

  source.fulfillment_service,

  source.admin_graphql_api_id,

  source.inventory_management,

  source.old_inventory_quantity

 

  );

 
