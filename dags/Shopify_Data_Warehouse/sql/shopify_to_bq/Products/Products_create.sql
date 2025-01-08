
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Products`
PARTITION BY DATE_TRUNC(Product_created_at, DAY)
CLUSTER BY Product_id
OPTIONS(
  description = "Products table is partitioned on Product_created_at",
  require_partition_filter = FALSE
)
 AS 
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
