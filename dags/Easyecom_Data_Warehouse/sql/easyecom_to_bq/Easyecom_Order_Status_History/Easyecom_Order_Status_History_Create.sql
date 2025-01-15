CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Order_Status_History`
PARTITION BY DATE_TRUNC(order_date,day)
CLUSTER BY order_status
OPTIONS(
 description = "Orders status history table is partitioned on Order date at day level and clustered by order status",
 require_partition_filter = FALSE
 )
 AS 
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