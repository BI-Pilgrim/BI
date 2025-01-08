

MERGE INTO  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.tender_transactions` AS target

USING (
SELECT 
distinct
_airbyte_extracted_at,
id as tender_transaction_id,
test,
CAST(ROUND(CAST(amount as FLOAT64))AS INT64) as transaction_amount,
user_id,
currency,
order_id,
shop_url,
processed_at as transaction_processed_at ,
payment_method,
json_extract_scalar(payment_details,"$.credit_card_company") as credit_card_company,
json_extract_scalar(payment_details,"$.credit_card_number") as credit_card_number,
remote_reference

FROM
`shopify-pubsub-project.pilgrim_bi_airbyte.tender_transactions`


  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.tender_transaction_id = source.tender_transaction_id

WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at 
THEN UPDATE SET

target._airbyte_extracted_at = source._airbyte_extracted_at,
target.tender_transaction_id = source.tender_transaction_id,
target.test = source.test,
target.transaction_amount = source.transaction_amount,
target.user_id = source.user_id,
target.currency = source.currency,
target.order_id = source.order_id,
target.shop_url = source.shop_url,
target.transaction_processed_at  = source.transaction_processed_at,
target.payment_method = source.payment_method,
target.credit_card_company = source.credit_card_company,
target.credit_card_number = source.credit_card_number,
target.remote_reference = source.remote_reference


WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
tender_transaction_id,
test,
transaction_amount,
user_id,
currency,
order_id,
shop_url,
transaction_processed_at ,
payment_method,
credit_card_company,
credit_card_number,
remote_reference

 )

  VALUES (
source._airbyte_extracted_at,
source.tender_transaction_id,
source.test,
source.transaction_amount,
source.user_id,
source.currency,
source.order_id,
source.shop_url,
source.transaction_processed_at ,
source.payment_method,
source.credit_card_company,
source.credit_card_number,
source.remote_reference
  )
