
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_customers` AS target

USING (
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
COALESCE(MAX(case when key='profession' then value end),'NIL') as Profession_field,
COALESCE(MAX(case when key='expected_gender' then value end),'NIL') as Gender_der_field,
COALESCE(MAX(case when key='dob' then value end),'NIL') as DOB_field,
COALESCE(MAX(case when key='language' then value end),'NIL') as Language_field,
COALESCE(MAX(case when key='in_love_with' then value end),'NIL') as loved_product_field
--admin_graphql_api_id as admin_graphql_api_id

 FROM  `shopify-pubsub-project.pilgrim_bi_airbyte.metafield_customers`
 WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 group by ALL
  
 ) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.customer_id = source.customer_id,
target.metafield_shop_url = source.metafield_shop_url,
target.metafield_owner_resource = source.metafield_owner_resource,
target.customer_created_at = source.customer_created_at,
target.customer_updated_at = source.customer_updated_at,
target.Gender_field = source.Gender_field,
target.Personalization_field = source.Personalization_field,
target.Concerns_field = source.Concerns_field,
target.Profession_field = source.Profession_field,
target.Gender_der_field = source.Gender_der_field,
target.DOB_field = source.DOB_field,
target.Language_field = source.Language_field,
target.loved_product_field = source.loved_product_field
--target.admin_graphql_api_id = source.admin_graphql_api_id

WHEN NOT MATCHED THEN INSERT (

_airbyte_extracted_at,
customer_id,
metafield_shop_url,
metafield_owner_resource,
customer_created_at,
customer_updated_at,
Gender_field,
Personalization_field,
Concerns_field,
Profession_field,
Gender_der_field,
DOB_field,
Language_field,
loved_product_field
--admin_graphql_api_id

   )
  VALUES (
source._airbyte_extracted_at,
source.customer_id,
source.metafield_shop_url,
source.metafield_owner_resource,
source.customer_created_at,
source.customer_updated_at,
source.Gender_field,
source.Personalization_field,
source.Concerns_field,
source.Profession_field,
source.Gender_der_field,
source.DOB_field,
source.Language_field,
source.loved_product_field
--source.admin_graphql_api_id 
  )
