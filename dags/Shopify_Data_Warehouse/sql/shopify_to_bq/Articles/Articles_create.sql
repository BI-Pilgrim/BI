
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Articles`
PARTITION BY DATE_TRUNC(Article_created_at,day)
CLUSTER BY blog_id
OPTIONS(
 description = "Articles table is partitioned on Articles created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id,
tags as Article_tag,
title as Article_title,
author as Article_author,
handle,
blog_id,
user_id,
shop_url,
body_html,
created_at as Article_created_at,
deleted_at as Article_deleted_at,
updated_at as Article_updated_at,
published_at as Article_published_at,
summary_html,
deleted_message,
template_suffix,
deleted_description,
admin_graphql_api_id

from `shopify-pubsub-project.pilgrim_bi_airbyte.articles`
