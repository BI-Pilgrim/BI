
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_Articles`
PARTITION BY DATE_TRUNC(Metafield_article_created_at,day)
CLUSTER BY Metafield_article_id
OPTIONS(
 description = "Metafield article table is partitioned on Metafield created at ",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id as Metafield_article_id,
key as Metafield_article_key,
type as Metafield_type,
value as Metafield_value,
owner_id,
shop_url,
namespace,
created_at as Metafield_article_created_at,
updated_at as Metafield_article_updated_at,
owner_resource,
admin_graphql_api_id

FROM
`shopify-pubsub-project.pilgrim_bi_airbyte.metafield_articles`
