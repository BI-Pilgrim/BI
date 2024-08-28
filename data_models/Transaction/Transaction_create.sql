
CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Transactions`
PARTITION BY DATE_TRUNC(Trans_created_at,day)
CLUSTER BY Trans_kind,Trans_status
OPTIONS(
 description = "Transaction table is partitioned on Transaction created at",
 require_partition_filter = False
 )
 AS 
SELECT 
_airbyte_extracted_at,
test as Trans_test,
kind as Trans_kind,
status as Trans_status,
gateway as Trans_gateway,
amount as Trans_amount,
created_at as Trans_created_at,
processed_at as Trans_processed_at,
order_id as Trans_order_id,
id as Trans_id,
payment_id as Trans_payment_id,
JSON_EXTRACT(payment_details, '$.avs_result_code') AS payment_avs_result_code,
JSON_EXTRACT(payment_details, '$.credit_card_bin') AS payment_credit_card_bin,
JSON_EXTRACT(payment_details, '$.credit_card_company') AS payment_credit_card_company,
JSON_EXTRACT(payment_details, '$.credit_card_expiration_month') AS payment_credit_card_expiration_month,
JSON_EXTRACT(payment_details, '$.credit_card_expiration_year') AS payment_credit_card_expiration_year,
JSON_EXTRACT(payment_details, '$.credit_card_name') AS payment_credit_card_name,
JSON_EXTRACT(payment_details, '$.credit_card_number') AS payment_credit_card_number,
JSON_EXTRACT(payment_details, '$.credit_card_wallet') AS payment_credit_card_wallet,
JSON_EXTRACT(payment_details, '$.cvv_result_code') AS payment_cvv_result_code,


FROM  `shopify-pubsub-project.airbyte711.transactions`

