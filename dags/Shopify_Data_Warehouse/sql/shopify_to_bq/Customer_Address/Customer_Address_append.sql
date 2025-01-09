
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customer_Address` AS target

USING (
  SELECT DISTINCT
    _airbyte_extracted_at,
    id,
    zip AS Pincode,
    city AS City,
    name AS Customer_name,
    phone,
    company,
    country,
    address1 AS Address_line1,
    address2 AS Address_line2,
    province,
    shop_url,
    last_name,
    first_name,
    updated_at AS Cust_updated_at,
    customer_id,
    country_code,
    province_code,
    country_name
  FROM `shopify-pubsub-project.pilgrim_bi_airbyte.customer_address`
  WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
) AS source

ON target.id = source.id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
  target._airbyte_extracted_at = source._airbyte_extracted_at,
  target.Pincode = source.Pincode,
  target.City = source.City,
  target.Customer_name = source.Customer_name,
  target.phone = source.phone,
  target.company = source.company,
  target.country = source.country,
  target.Address_line1 = source.Address_line1,
  target.Address_line2 = source.Address_line2,
  target.province = source.province,
  target.shop_url = source.shop_url,
  target.last_name = source.last_name,
  target.first_name = source.first_name,
  target.Cust_updated_at = source.Cust_updated_at,
  target.customer_id = source.customer_id,
  target.country_code = source.country_code,
  target.province_code = source.province_code,
  target.country_name = source.country_name

WHEN NOT MATCHED THEN INSERT (
  _airbyte_extracted_at,
  id,
  Pincode,
  City,
  Customer_name,
  phone,
  company,
  country,
  Address_line1,
  Address_line2,
  province,
  shop_url,
  last_name,
  first_name,
  Cust_updated_at,
  customer_id,
  country_code,
  province_code,
  country_name
) VALUES (
  source._airbyte_extracted_at,
  source.id,
  source.Pincode,
  source.City,
  source.Customer_name,
  source.phone,
  source.company,
  source.country,
  source.Address_line1,
  source.Address_line2,
  source.province,
  source.shop_url,
  source.last_name,
  source.first_name,
  source.Cust_updated_at,
  source.customer_id,
  source.country_code,
  source.province_code,
  source.country_name
);
