
CREATE OR REPLACE TABLE `shopify-pubsub-project.Shopify_staging.Metafield_customers`
PARTITION BY DATE_TRUNC(customer_created_at,day)
-- CLUSTER BY 
OPTIONS(
 description = "Metafield Customer table is partitioned on customer created at day level",
 require_partition_filter = FALSE
 )
 AS 
SELECT 
-- CAST(id AS STRING) as metafield_id,
-- key as metafield_key,
-- type as metafield_type,
-- value as metafield_value,
-- namespace as metafield_namespace,


CAST(owner_id AS STRING) as customer_id,
shop_url as metafield_shop_url,
owner_resource as metafield_owner_resource,
Min(created_at) as customer_created_at,
Max(updated_at) as customer_updated_at,
min(_airbyte_extracted_at) as _airbyte_extracted_at,
COALESCE(MAX(case when key='gender' then value end),'NIL') as Gender_field,
COALESCE(MAX(case when key='personalization_products' then value end),'NIL') as Personalization_field,
COALESCE(MAX(case when key='concerns' then value end),'NIL') as Concerns_field,


 FROM  `shopify-pubsub-project.airbyte711.metafield_customers`
group by ALL

