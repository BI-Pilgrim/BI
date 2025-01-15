MERGE INTO  `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Order_Status_History` AS target

USING (

  select
  distinct
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
   FROM `shopify-pubsub-project.easycom.orders`

   WHERE date(order_date) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
 
 ) AS source
ON target.invoice_id = source.invoice_id
and target.last_update_date = source.last_update_date

WHEN MATCHED AND source.order_date > target.order_date THEN UPDATE SET

target.invoice_id = source.invoice_id,
target.order_id = source.order_id,
target.reference_code = source.reference_code,
target.order_date = source.order_date,
target.invoice_date = source.invoice_date,
target.order_status = source.order_status,
target.order_status_id = source.order_status_id,
target.fulfillable_status = source.fulfillable_status,
target.queue_status = source.queue_status,
target.last_update_date = source.last_update_date,
target.ee_extracted_at = source.ee_extracted_at

WHEN NOT MATCHED THEN INSERT (
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
  VALUES (
source.invoice_id,
source.order_id,
source.reference_code,
source.order_date,
source.invoice_date,
source.order_status,
source.order_status_id,
source.fulfillable_status,
source.queue_status,
source.last_update_date,
source.ee_extracted_at

  )

