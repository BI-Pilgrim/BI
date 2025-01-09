
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Locations`
PARTITION BY DATE_TRUNC(location_created_at,day)
CLUSTER BY location_id,location_name
OPTIONS(
 description = "Locations table is partitioned on location created at ",
 require_partition_filter = False
 )
 AS 
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
