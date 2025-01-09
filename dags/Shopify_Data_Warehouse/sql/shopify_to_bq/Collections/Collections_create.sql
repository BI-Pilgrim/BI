
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Collections`
PARTITION BY DATE_TRUNC(Collection_Created_at,day)
CLUSTER BY sort_order
OPTIONS(
 description = "Collections table is partitioned on collection created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id,
title as Collection_title,
handle,
shop_url,
body_html,
sort_order,
updated_at as Collection_updated_at,
published_at as Collection_Created_at,
products_count,
collection_type,
published_scope,
template_suffix,
admin_graphql_api_id

FROM  `shopify-pubsub-project.pilgrim_bi_airbyte.collections`
