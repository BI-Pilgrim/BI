

MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Pages` AS target

USING (
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
    
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.Page_id = source.Page_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Page_id = source.Page_id,
target.Page_title = source.Page_title,
target.Page_author = source.Page_author,
target.Page_handle = source.Page_handle,
target.shop_id = source.shop_id,
target.shop_url = source.shop_url,
target.body_html = source.body_html,
target.Page_created_at = source.Page_created_at,
target.Page_deleted_at = source.Page_deleted_at,
target.Page_updated_at = source.Page_updated_at,
target.Page_published_at = source.Page_published_at,
target.deleted_message = source.deleted_message,
target.template_suffix = source.template_suffix,
target.deleted_description = source.deleted_description,
target.admin_graphql_api_id = source.admin_graphql_api_id


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Page_id,
Page_title,
Page_author,
Page_handle,
shop_id,
shop_url,
body_html,
Page_created_at,
Page_deleted_at,
Page_updated_at,
Page_published_at,
deleted_message,
template_suffix,
deleted_description,
admin_graphql_api_id
   )

  VALUES (
source._airbyte_extracted_at,
source.Page_id,
source.Page_title,
source.Page_author,
source.Page_handle,
source.shop_id,
source.shop_url,
source.body_html,
source.Page_created_at,
source.Page_deleted_at,
source.Page_updated_at,
source.Page_published_at,
source.deleted_message,
source.template_suffix,
source.deleted_description,
source.admin_graphql_api_id

  )
