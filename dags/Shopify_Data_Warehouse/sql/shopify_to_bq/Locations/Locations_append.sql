

MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Locations` AS target

USING (
  SELECT 
distinct
_airbyte_extracted_at,
id as location_id,
zip as location_zip,
city,
name as location_name,
phone,
active,
legacy,
country,
address1,
address2,
province,
shop_url,
created_at as location_created_at,
updated_at as location_updated_at,
country_code,
country_name,
province_code,
admin_graphql_api_id,
localized_country_name,
localized_province_name
from `shopify-pubsub-project.pilgrim_bi_airbyte.locations`
    
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.location_id = source.location_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.location_id = source.location_id,
target.location_zip = source.location_zip,
target.city = source.city,
target.location_name = source.location_name,
target.phone = source.phone,
target.active = source.active,
target.legacy = source.legacy,
target.country = source.country,
target.address1 = source.address1,
target.address2 = source.address2,
target.province = source.province,
target.shop_url = source.shop_url,
target.location_created_at = source.location_created_at,
target.location_updated_at = source.location_updated_at,
target.country_code = source.country_code,
target.country_name = source.country_name,
target.province_code = source.province_code,
target.admin_graphql_api_id = source.admin_graphql_api_id,
target.localized_country_name = source.localized_country_name,
target.localized_province_name = source.localized_province_name

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
location_id,
location_zip,
city,
location_name,
phone,
active,
legacy,
country,
address1,
address2,
province,
shop_url,
location_created_at,
location_updated_at,
country_code,
country_name,
province_code,
admin_graphql_api_id,
localized_country_name,
localized_province_name
   )
  VALUES (
source._airbyte_extracted_at,
source.location_id,
source.location_zip,
source.city,
source.location_name,
source.phone,
source.active,
source.legacy,
source.country,
source.address1,
source.address2,
source.province,
source.shop_url,
source.location_created_at,
source.location_updated_at,
source.country_code,
source.country_name,
source.province_code,
source.admin_graphql_api_id,
source.localized_country_name,
source.localized_province_name

  )
