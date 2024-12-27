
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.tender_transactions`
PARTITION BY DATE_TRUNC(transaction_processed_at,day)
CLUSTER BY tender_transaction_id
OPTIONS(
 description = "Tender transaction table is partitioned on transaction processed at ",
 require_partition_filter = False
 )
 AS 
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
