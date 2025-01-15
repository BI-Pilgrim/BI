

CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.All_Return_Orders`
PARTITION BY DATE_TRUNC(order_date,day)
-- CLUSTER BY 
OPTIONS(
 description = "All Return Order table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select
  CAST(invoice_id AS STRING) AS invoice_id,
  CAST(order_id AS STRING) AS order_id,
  reference_code,
  company_name,
  CAST(ware_house_id AS STRING) AS ware_house_id,
  seller_gst,
  forward_shipment_pickup_address,
  forward_shipment_pickup_city,
  forward_shipment_pickup_state,
  forward_shipment_pickup_pin_code,
  forward_shipment_pickup_country,
  order_type,
  order_type_key,
  replacement_order,
  marketplace,
  CAST(marketplace_id AS STRING) AS marketplace_id,
  CAST(salesman_user_id AS STRING) AS salesman_user_id,
  order_date,
  invoice_date,
  import_date,
  last_update_date,
  manifest_date,
  return_date,
  manifest_no,
  invoice_number,
  marketplace_credit_note_num,
  marketplace_invoice_num,
  CAST(batch_id AS STRING) AS batch_id,
  batch_created_at,
  payment_mode,
  CAST(payment_mode_id AS STRING) AS payment_mode_id,
  buyer_gst,
  forward_shipment_customer_name,
  forward_shipment_customer_contact_num,
  forward_shipment_customer_address_line_1,
  forward_shipment_customer_address_line_2,
  forward_shipment_customer_city,
  forward_shipment_customer_pin_code,
  forward_shipment_customer_state,
  forward_shipment_customer_country,
  forward_shipment_customer_email,
  forward_shipment_billing_name,
  forward_shipment_billing_address_1,
  forward_shipment_billing_address_2,
  forward_shipment_billing_city,
  forward_shipment_billing_state,
  forward_shipment_billing_pin_code,
  forward_shipment_billing_country,
  forward_shipment_billing_mobile,
  order_quantity,
  total_invoice_amount,
  total_invoice_tax,
  invoice_collectable_amount,
  CAST(credit_note_id AS STRING) AS credit_note_id,
  credit_note_date,
  credit_note_number,
  ee_extracted_at
  FROM `shopify-pubsub-project.easycom.all_return_orders`
