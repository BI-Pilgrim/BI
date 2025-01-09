

MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Collections` AS target

USING (
  SELECT DISTINCT
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
  WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.id = source.id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
  target._airbyte_extracted_at = source._airbyte_extracted_at,
  target.id = source.id,
  target.Collection_title = source.Collection_title,
  target.handle = source.handle,
  target.shop_url = source.shop_url,
  target.body_html = source.body_html,
  target.sort_order = source.sort_order,
  target.Collection_updated_at = source.Collection_updated_at,
  target.Collection_Created_at = source.Collection_Created_at,
  target.products_count = source.products_count,
  target.collection_type = source.collection_type,
  target.published_scope = source.published_scope,
  target.template_suffix = source.template_suffix,
  target.admin_graphql_api_id = source.admin_graphql_api_id
  

WHEN NOT MATCHED THEN INSERT (
  _airbyte_extracted_at,
  id,
  Collection_title,
  handle,
  shop_url,
  body_html,
  sort_order,
  Collection_updated_at,
  Collection_Created_at,
  products_count,
  collection_type,
  published_scope,
  template_suffix,
  admin_graphql_api_id
) VALUES (
  source._airbyte_extracted_at,
  source.id,
  source.Collection_title,
  source.handle,
  source.shop_url,
  source.body_html,
  source.sort_order,
  source.Collection_updated_at,
  source.Collection_Created_at,
  source.products_count,
  source.collection_type,
  source.published_scope,
  source.template_suffix,
  source.admin_graphql_api_id
);
