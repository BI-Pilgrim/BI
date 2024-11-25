
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customer_Address`
PARTITION BY DATE_TRUNC(Cust_updated_at,day)
CLUSTER BY id
OPTIONS(
 description = "Customer Address table is partitioned on Customer Updated at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
id,
zip as Pincode,
city as City,
name as Customer_name,
phone,
company,
country,
address1 as Address_line1,
address2 as Address_line2,
province,
shop_url,
last_name,
first_name,
updated_at as Cust_updated_at,
customer_id,
country_code,
province_code,
country_name,


FROM  `shopify-pubsub-project.pilgrim_bi_airbyte.customer_address`
