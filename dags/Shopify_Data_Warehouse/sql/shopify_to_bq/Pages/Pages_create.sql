

CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Pages`
PARTITION BY DATE_TRUNC(Page_created_at,day)
CLUSTER BY Page_id
OPTIONS(
 description = "Pages table is partitioned on  Page_created_at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id as Page_id,
title as Page_title,
author as Page_author,
handle as Page_handle,
shop_id,
shop_url,
CAST(JSON_EXTRACT_SCALAR(body_html) AS STRING) AS body_html,
created_at as Page_created_at,
deleted_at as Page_deleted_at,
updated_at as Page_updated_at,
published_at as Page_published_at,
deleted_message,
template_suffix,
deleted_description,
admin_graphql_api_id

from `shopify-pubsub-project.pilgrim_bi_airbyte.pages`
