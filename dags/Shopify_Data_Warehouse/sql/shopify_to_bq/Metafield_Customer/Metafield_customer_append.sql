
MERGE INTO `shopify-pubsub-project.Shopify_staging.Metafield_customers` AS target

USING (
  select
  *
  from
    (  SELECT 
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
    
    FROM `shopify-pubsub-project.airbyte711.metafield_customers`
    group by ALL)
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.customer_id = source.customer_id,
target.metafield_shop_url = source.metafield_shop_url,
target.customer_created_at = source.customer_created_at,
target.customer_updated_at = source.customer_updated_at,
target.metafield_owner_resource = source.metafield_owner_resource,
target.Gender_field = source.Gender_field,
target.Personalization_field = source.Personalization_field,
target.Concerns_field = source.Concerns_field

WHEN NOT MATCHED THEN INSERT (

_airbyte_extracted_at,
customer_id,
metafield_shop_url,
customer_created_at,
customer_updated_at,
metafield_owner_resource,
Gender_field,
Personalization_field,
Concerns_field

   )
  VALUES (
source._airbyte_extracted_at,
source.customer_id,
source.metafield_shop_url,
source.customer_created_at,
source.customer_updated_at,
source.metafield_owner_resource,
source.Gender_field,
source.Personalization_field,
source.Concerns_field
  )




