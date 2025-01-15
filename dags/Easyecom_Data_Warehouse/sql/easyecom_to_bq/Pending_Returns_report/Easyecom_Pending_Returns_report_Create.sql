CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Pending_Returns_report`
PARTITION BY DATE_TRUNC(Order_Date,day)
-- CLUSTER BY 
OPTIONS(
 description = "Pending Return Report table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select
  Client_Name,
  Client_Location,
  Marketplace,
  Party,
  Order_Number,
  Order_Item_ID,
  Invoice_Number,
  Channel_return_reason,
  SAFE_CAST(Order_Date AS DATETIME) AS Order_Date,
  SAFE_CAST(Invoice_Date AS DATETIME) AS Invoice_Date,
  SAFE_CAST(Return_Initiated_Date AS DATETIME) AS Return_Initiated_Date,
  SAFE_CAST(MP_Delivered_Date AS DATETIME) AS MP_Delivered_Date,
  sku,
  SAFE_CAST(Return_Quantity as FLOAT64) as Return_Quantity,
  companyProductId,
  Return_Status,
  Return_Type,
  Invoice_Id,
  Myntra_Packet_Order_Id,
  Packet_Amount,
  Reverse_AWB_Number,
  Reverse_Carrier_Name,
  Reverse_Carrier_Aggregator,
  Forward_Awb_Number,
  Forward_Carrier_Name,
  Forward_Carrier_Aggregator,
  Created_By,
  MP_RefId,
  External_Order_Code,
  MP_Alias,
  Gate_Entry_Id,
  Gate_Number,
  Gate_Entry_Created_Date,
  Gate_Entry_Shipment_Received_Date,
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  inventory_type,
  ee_extracted_at,

  FROM `shopify-pubsub-project.easycom.pending_returns_report`

