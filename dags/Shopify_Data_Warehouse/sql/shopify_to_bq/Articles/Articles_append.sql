MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Articles` AS target  

USING (

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

WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)

) AS source

ON target.id = source.id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at

THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.id = source.id,
target.Article_tag = source.Article_tag,
target.Article_title = source.Article_title,
target.Article_author = source.Article_author,
target.handle = source.handle,
target.blog_id = source.blog_id,
target.user_id = source.user_id,
target.shop_url = source.shop_url,
target.body_html = source.body_html,
target.Article_created_at = source.Article_created_at,
target.Article_deleted_at = source.Article_deleted_at,
target.Article_updated_at = source.Article_updated_at,
target.Article_published_at = source.Article_published_at,
target.summary_html = source.summary_html,
target.deleted_message = source.deleted_message,
target.template_suffix = source.template_suffix,
target.deleted_description = source.deleted_description,
target.admin_graphql_api_id = source.admin_graphql_api_id

WHEN NOT MATCHED THEN INSERT (

_airbyte_extracted_at,

id,

Article_tag,

Article_title,

Article_author,

handle,

blog_id,

user_id,

shop_url,

body_html,

Article_created_at,

Article_deleted_at,

Article_updated_at,

Article_published_at,

summary_html,

deleted_message,

template_suffix,

deleted_description,

admin_graphql_api_id

   

  )

  VALUES (

  source._airbyte_extracted_at,

  source.id,

  source.Article_tag,

  source.Article_title,

  source.Article_author,

  source.handle,

  source.blog_id,

  source.user_id,

  source.shop_url,

  source.body_html,

  source.Article_created_at,

  source.Article_deleted_at,

  source.Article_updated_at,

  source.Article_published_at,

  source.summary_html,

  source.deleted_message,

  source.template_suffix,

  source.deleted_description,

  source.admin_graphql_api_id

 

  );
