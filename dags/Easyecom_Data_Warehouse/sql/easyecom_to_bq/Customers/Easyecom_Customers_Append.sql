MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Customers` AS TARGET
USING
(
SELECT
  CAST(company_invoice_group_id AS STRING) AS company_invoice_group_id,
  CAST(c_id AS STRING) AS c_id,
  CAST(company_name AS STRING) AS company_name,
  CAST(pricing_group AS STRING) AS pricing_group,
  CAST(customer_support_email AS STRING) AS customer_support_email,
  CAST(customer_support_contact AS STRING) AS customer_support_contact,
  CAST(brand_description AS STRING) AS brand_description,
  CAST(currency_code AS STRING) AS currency_code,
  CAST(gst_num AS STRING) AS gst_num,
  CAST(type_of_customer AS STRING) AS type_of_customer,
  CAST(billing_street AS STRING) AS billing_street,
  CAST(billing_city AS STRING) AS billing_city,
  CAST(billing_zipcode AS STRING) AS billing_zipcode,
  CAST(billing_state AS STRING) AS billing_state,
  CAST(billing_country AS STRING) AS billing_country,
  CAST(dispatch_street AS STRING) AS dispatch_street,
  CAST(dispatch_city AS STRING) AS dispatch_city,
  CAST(dispatch_zipcode AS STRING) AS dispatch_zipcode,
  CAST(dispatch_state AS STRING) AS dispatch_state,
  CAST(dispatch_country AS STRING) AS dispatch_country,
  ee_extracted_at,
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITiON BY ee_extracted_at ORDER BY ee_extracted_at DESC) as row_num
FROM `shopify-pubsub-project.easycom.customers`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE.c_id = TARGET.c_id
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
THEN UPDATE SET
TARGET.company_invoice_group_id = SOURCE.company_invoice_group_id,
TARGET.c_id = SOURCE.c_id,
TARGET.company_name = SOURCE.company_name,
TARGET.pricing_group = SOURCE.pricing_group,
TARGET.customer_support_email = SOURCE.customer_support_email,
TARGET.customer_support_contact = SOURCE.customer_support_contact,
TARGET.brand_description = SOURCE.brand_description,
TARGET.currency_code = SOURCE.currency_code,
TARGET.gst_num = SOURCE.gst_num,
TARGET.type_of_customer = SOURCE.type_of_customer,
TARGET.billing_street = SOURCE.billing_street,
TARGET.billing_city = SOURCE.billing_city,
TARGET.billing_zipcode = SOURCE.billing_zipcode,
TARGET.billing_state = SOURCE.billing_state,
TARGET.billing_country = SOURCE.billing_country,
TARGET.dispatch_street = SOURCE.dispatch_street,
TARGET.dispatch_city = SOURCE.dispatch_city,
TARGET.dispatch_zipcode = SOURCE.dispatch_zipcode,
TARGET.dispatch_state = SOURCE.dispatch_state,
TARGET.dispatch_country = SOURCE.dispatch_country,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
company_invoice_group_id,
c_id,
company_name,
pricing_group,
customer_support_email,
customer_support_contact,
brand_description,
currency_code,
gst_num,
type_of_customer,
billing_street,
billing_city,
billing_zipcode,
billing_state,
billing_country,
dispatch_street,
dispatch_city,
dispatch_zipcode,
dispatch_state,
dispatch_country,
ee_extracted_at
)
VALUES
(
SOURCE.company_invoice_group_id,
SOURCE.c_id,
SOURCE.company_name,
SOURCE.pricing_group,
SOURCE.customer_support_email,
SOURCE.customer_support_contact,
SOURCE.brand_description,
SOURCE.currency_code,
SOURCE.gst_num,
SOURCE.type_of_customer,
SOURCE.billing_street,
SOURCE.billing_city,
SOURCE.billing_zipcode,
SOURCE.billing_state,
SOURCE.billing_country,
SOURCE.dispatch_street,
SOURCE.dispatch_city,
SOURCE.dispatch_zipcode,
SOURCE.dispatch_state,
SOURCE.dispatch_country,
SOURCE.ee_extracted_at
)
