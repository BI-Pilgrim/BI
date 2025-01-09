
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_pages`
PARTITION BY DATE_TRUNC(Metafield_page_created_at,day)
CLUSTER BY Metafield_page_id, namespace
OPTIONS(
 description = "Metafield Pages table is partitioned on  Metafield_page_created_at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id as Metafield_page_id,
key as Metafield_page_key,
type as Page_type,
value as Page_value,
owner_id,
shop_url,
namespace,
created_at as Metafield_page_created_at,
updated_at as Metafield_page_updated_at,
description as Page_description,
owner_resource,
admin_graphql_api_id

from `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_pages`
