
CREATE or replace TABLE `shopify-pubsub-project.Shopify_staging.Transactions`
PARTITION BY DATE_TRUNC(Trans_created_at,day)
CLUSTER BY Trans_kind,Trans_status
OPTIONS(
 description = "Transaction table is partitioned on Transaction created at",
 require_partition_filter = False
 )
 AS 
SELECT 
distinct
_airbyte_extracted_at,
test as Trans_test,
kind as Trans_kind,
status as Trans_status,
gateway as Trans_gateway,
amount as Trans_amount,
created_at as Trans_created_at,
processed_at as Trans_processed_at,
CAST(order_id as STRING) as Trans_order_id,
CAST(id as STRING) as Trans_id,
payment_id as Trans_payment_id,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.avs_result_code') AS STRING) AS payment_avs_result_code,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.credit_card_bin') AS STRING) AS payment_credit_card_bin,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.credit_card_company') AS STRING) AS payment_credit_card_company,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.credit_card_expiration_month') AS STRING) AS payment_credit_card_expiration_month,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.credit_card_expiration_year') AS STRING) AS payment_credit_card_expiration_year,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.credit_card_name') AS STRING) AS payment_credit_card_name,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.credit_card_number') AS STRING) AS payment_credit_card_number,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.credit_card_wallet') AS STRING) AS payment_credit_card_wallet,
CAST(JSON_EXTRACT_SCALAR(payment_details, '$.cvv_result_code') AS STRING) AS payment_cvv_result_code,


FROM  `shopify-pubsub-project.airbyte711.transactions`

