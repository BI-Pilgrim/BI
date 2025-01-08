CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_customers`
PARTITION BY DATE_TRUNC(customer_created_at,day)
CLUSTER BY customer_id
OPTIONS(
 description = "Metafield Customer table is partitioned on customer created at day level",
 require_partition_filter = FALSE
 )
 AS 
SELECT 
distinct
CAST(owner_id AS STRING) as customer_id,
shop_url as metafield_shop_url,
owner_resource as metafield_owner_resource,
Min(created_at) as customer_created_at,
Max(updated_at) as customer_updated_at,
min(_airbyte_extracted_at) as _airbyte_extracted_at,
COALESCE(MAX(case when key='gender' then value end),'NIL') as Gender_field,
COALESCE(MAX(case when key='personalization_products' then value end),'NIL') as Personalization_field,
COALESCE(MAX(case when key='concerns' then value end),'NIL') as Concerns_field,
admin_graphql_api_id as admin_graphql_api_id

 FROM  `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_customers`
group by ALL
