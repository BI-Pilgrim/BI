MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Order_Status_History` AS TARGET
USING
(
select
  SAFE_CAST(invoice_id as STRING) as invoice_id,
  SAFE_CAST(order_id as STRING) as order_id,
  reference_code,
  order_date,
  invoice_date,
  order_status,
  order_status_id,
  fulfillable_status,
  queue_status,
  last_update_date,
  ee_extracted_at
FROM
(
SELECT
*,
<<<<<<< Updated upstream
ROW_NUMBER() OVER(PARTITTION BY  ORDER BY ) AS row_num
FROM `shopify-pubsub-project.easycom.orders`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
) AS SOURCE
ON TARGET. = SOURCE.
WHEN MATCHED AND TARGET. > SOURCE.
=======
ROW_NUMBER() OVER(PARTITION BY ee_extracted_at ORDER BY ee_extracted_at) AS row_num
FROM `shopify-pubsub-project.easycom.orders`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
) AS SOURCE
ON TARGET.order_id = SOURCE.order_id
WHEN MATCHED AND TARGET.order_date < SOURCE.order_date
>>>>>>> Stashed changes
THEN UPDATE SET
TARGET.invoice_id = SOURCE.invoice_id,
TARGET.order_id = SOURCE.order_id,
TARGET.reference_code = SOURCE.reference_code,
TARGET.order_date = SOURCE.order_date,
TARGET.invoice_date = SOURCE.invoice_date,
TARGET.order_status = SOURCE.order_status,
TARGET.order_status_id = SOURCE.order_status_id,
TARGET.fulfillable_status = SOURCE.fulfillable_status,
TARGET.queue_status = SOURCE.queue_status,
TARGET.last_update_date = SOURCE.last_update_date,
<<<<<<< Updated upstream
TARGET.  ee_extracted_at = SOURCE.  ee_extracted_at,
=======
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
>>>>>>> Stashed changes
WHEN NOT MATCHED
THEN INSERT
(
  invoice_id,
  order_id,
  reference_code,
  order_date,
  invoice_date,
  order_status,
  order_status_id,
  fulfillable_status,
  queue_status,
  last_update_date,
  ee_extracted_at
)
VALUES
(
SOURCE.invoice_id,
SOURCE.order_id,
SOURCE.reference_code,
SOURCE.order_date,
SOURCE.invoice_date,
SOURCE.order_status,
SOURCE.order_status_id,
SOURCE.fulfillable_status,
SOURCE.queue_status,
SOURCE.last_update_date,
<<<<<<< Updated upstream
SOURCE.  ee_extracted_at
=======
SOURCE.ee_extracted_at
>>>>>>> Stashed changes
)