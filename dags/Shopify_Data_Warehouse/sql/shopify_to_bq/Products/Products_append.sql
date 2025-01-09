
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Products` AS target

USING (
SELECT  
distinct
_airbyte_extracted_at,
id as Product_id,
JSON_EXTRACT_SCALAR(seo, '$.description') AS SEO_description,
JSON_EXTRACT_SCALAR(seo, '$.title') AS SEO_title,
tags as Product_tags,
title as Product_title,
handle,
status as Product_status,
vendor,
cast(json_extract_scalar(item,'$.id') as STRING) as Product_variant_id,
body_html,
created_at as Product_created_at,
updated_at as Product_updated_at,
description as Product_description,
media_count,
is_gift_card,
product_type,
published_at,
json_extract_scalar(featured_image,'$.url') as Product_image_url,
CAST(JSON_EXTRACT_SCALAR(price_range_v2, '$.max_variant_price.amount') AS FLOAT64) AS max_variant_price,
CAST(JSON_EXTRACT_SCALAR(price_range_v2, '$.min_variant_price.amount') AS FLOAT64) AS min_variant_price,
total_variants,
template_suffix,
total_inventory,
description_html,
online_store_url,
tracks_inventory,
legacy_resource_id,
admin_graphql_api_id,
has_only_default_variant,
online_store_preview_url,
has_out_of_stock_variants
from
`shopify-pubsub-project.pilgrim_bi_airbyte.products`,
unnest(json_extract_array(variants)) as item

    
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.Product_variant_id = source.Product_variant_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Product_id = source.Product_id,
target.SEO_description = source.SEO_description,
target.SEO_title = source.SEO_title,
target.Product_tags = source.Product_tags,
target.Product_title = source.Product_title,
target.handle = source.handle,
target.Product_status = source.Product_status,
target.vendor = source.vendor,
target.Product_variant_id = source.Product_variant_id,
target.body_html = source.body_html,
target.Product_created_at = source.Product_created_at,
target.Product_updated_at = source.Product_updated_at,
target.Product_description = source.Product_description,
target.media_count = source.media_count,
target.is_gift_card = source.is_gift_card,
target.product_type = source.product_type,
target.published_at = source.published_at,
target.Product_image_url = source.Product_image_url,
target.max_variant_price = source.max_variant_price,
target.min_variant_price = source.min_variant_price,
target.total_variants = source.total_variants,
target.template_suffix = source.template_suffix,
target.total_inventory = source.total_inventory,
target.description_html = source.description_html,
target.online_store_url = source.online_store_url,
target.tracks_inventory = source.tracks_inventory,
target.legacy_resource_id = source.legacy_resource_id,
target.admin_graphql_api_id = source.admin_graphql_api_id,
target.has_only_default_variant = source.has_only_default_variant,
target.online_store_preview_url = source.online_store_preview_url,
target.has_out_of_stock_variants = source.has_out_of_stock_variants



WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Product_id,
SEO_description,
SEO_title,
Product_tags,
Product_title,
handle,
Product_status,
vendor,
Product_variant_id,
body_html,
Product_created_at,
Product_updated_at,
Product_description,
media_count,
is_gift_card,
product_type,
published_at,
Product_image_url,
max_variant_price,
min_variant_price,
total_variants,
template_suffix,
total_inventory,
description_html,
online_store_url,
tracks_inventory,
legacy_resource_id,
admin_graphql_api_id,
has_only_default_variant,
online_store_preview_url,
has_out_of_stock_variants
   )

  VALUES (
source._airbyte_extracted_at,
source.Product_id,
source.SEO_description,
source.SEO_title,
source.Product_tags,
source.Product_title,
source.handle,
source.Product_status,
source.vendor,
source.Product_variant_id,
source.body_html,
source.Product_created_at,
source.Product_updated_at,
source.Product_description,
source.media_count,
source.is_gift_card,
source.product_type,
source.published_at,
source.Product_image_url,
source.max_variant_price,
source.min_variant_price,
source.total_variants,
source.template_suffix,
source.total_inventory,
source.description_html,
source.online_store_url,
source.tracks_inventory,
source.legacy_resource_id,
source.admin_graphql_api_id,
source.has_only_default_variant,
source.online_store_preview_url,
source.has_out_of_stock_variants

  )
