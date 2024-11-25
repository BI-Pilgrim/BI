
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Transactions` AS target

USING (
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
    formattedGateway as formattedGateway,
    manuallyCapturable as manuallyCapturable,
    admin_graphql_api_id as admin_graphql_api_id


  FROM `shopify-pubsub-project.pilgrim_bi_airbyte.transactions`
  WHERE date(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.Trans_id = source.Trans_id
WHEN MATCHED AND source._airbyte_extracted_at > target._airbyte_extracted_at THEN UPDATE SET
target._airbyte_extracted_at = source._airbyte_extracted_at,
target.Trans_test = source.Trans_test,
target.Trans_kind = source.Trans_kind,
target.Trans_status = source.Trans_status,
target.Trans_gateway = source.Trans_gateway,
target.Trans_amount = source.Trans_amount,
target.Trans_created_at = source.Trans_created_at,
target.Trans_processed_at = source.Trans_processed_at,
target.Trans_order_id = source.Trans_order_id,
target.Trans_id = source.Trans_id,
target.Trans_payment_id = source.Trans_payment_id,
target.payment_avs_result_code = source.payment_avs_result_code,
target.payment_credit_card_bin = source.payment_credit_card_bin,
target.payment_credit_card_company = source.payment_credit_card_company,
target.payment_credit_card_expiration_month = source.payment_credit_card_expiration_month,
target.payment_credit_card_expiration_year = source.payment_credit_card_expiration_year,
target.payment_credit_card_name = source.payment_credit_card_name,
target.payment_credit_card_number = source.payment_credit_card_number,
target.payment_credit_card_wallet = source.payment_credit_card_wallet,
target.payment_cvv_result_code = source.payment_cvv_result_code,
target.formattedGateway = source.formattedGateway,
target.manuallyCapturable = source.manuallyCapturable,
target.admin_graphql_api_id = source.admin_graphql_api_id

WHEN NOT MATCHED THEN INSERT (
_airbyte_extracted_at,
Trans_test,
Trans_kind,
Trans_status,
Trans_gateway,
Trans_amount,
Trans_created_at,
Trans_processed_at,
Trans_order_id,
Trans_id,
Trans_payment_id,
payment_avs_result_code,
payment_credit_card_bin,
payment_credit_card_company,
payment_credit_card_expiration_month,
payment_credit_card_expiration_year,
payment_credit_card_name,
payment_credit_card_number,
payment_credit_card_wallet,
payment_cvv_result_code,
formattedGateway ,
manuallyCapturable,
admin_graphql_api_id
   )
  VALUES (
source._airbyte_extracted_at,
source.Trans_test,
source.Trans_kind,
source.Trans_status,
source.Trans_gateway,
source.Trans_amount,
source.Trans_created_at,
source.Trans_processed_at,
source.Trans_order_id,
source.Trans_id,
source.Trans_payment_id,
source.payment_avs_result_code,
source.payment_credit_card_bin,
source.payment_credit_card_company,
source.payment_credit_card_expiration_month,
source.payment_credit_card_expiration_year,
source.payment_credit_card_name,
source.payment_credit_card_number,
source.payment_credit_card_wallet,
source.payment_cvv_result_code,
source.formattedGateway,
source.manuallyCapturable,
source.admin_graphql_api_id

  )




