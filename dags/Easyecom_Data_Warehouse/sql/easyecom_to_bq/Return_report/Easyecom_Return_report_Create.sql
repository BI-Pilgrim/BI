

CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Returns_report`
PARTITION BY DATE_TRUNC(Order_Date,day)
CLUSTER BY Seller_Return_Reason
OPTIONS(
 description = "Return report table is partitioned on orderdate at day level",
 require_partition_filter = False
 )
 AS
select *
from(
SELECT distinct
  Client_Name,
  Client_Location,
  Marketplace,
  Party,
  Order_Number,
  Order_Item_ID,
  Invoice_Number,
  Seller_Return_Reason,
  Initiated_Reason,
  Channel_return_reason,
  Creditnote_Number,
  MP_Credit_Note_No,
  SAFE_CAST(Order_Date AS DATETIME) AS Order_Date,
  SAFE_CAST(Invoice_Date AS DATETIME) AS Invoice_Date,
  SAFE_CAST(Return_Date AS DATETIME) AS Return_Date,
  SAFE_CAST(System_Return_Entry_Date AS DATETIME) AS System_Return_Entry_Date,
  Parent_Sku,
  Sku_Type,
  Child_Sku,
  Serial_Number,
  Inventory_Status,
  -- SAFE_CAST(Return_Quantity AS FLOAT64) AS Return_Quantity,
  SAFE_CAST(CASE WHEN Return_Quantity = 'nan' THEN '0' ELSE Return_Quantity END AS FLOAT64) AS Return_Quantity,
  companyProductId,
  Invoice_Id,
  Awb_Number,
  Reverse_Carrier_Name,
  Reverse_Carrier_Aggregator,
  Forward_Awb_Number,
  Forward_Carrier_Name,
  Forward_Carrier_Aggregator,
  Created_By,
  ReturnQC_Marked_By,
  RTO_Delivered_Date,
  SAFE_CAST(JSON_EXTRACT_SCALAR(MP_RefId,'$.mpRefId') as STRING) AS MP_RefId,
  Return_Type,
  Comments,
  -- SAFE_CAST(Total_Credit_Note_Amount AS FLOAT64) AS Total_Credit_Note_Amount,
  SAFE_CAST((CASE WHEN Total_Credit_Note_Amount='nan' THEN 0 ELSE Total_Credit_Note_Amount END) AS FLOAT64) as Total_Credit_Note_Amount,
  External_Order_Code,
  MP_Alias,
  SAFE_CAST(MP_Delivered_Date AS DATETIME) AS MP_Delivered_Date,
  Gate_Entry_Id,
  Gate_Number,
  SAFE_CAST(Gate_Entry_Created_Date AS DATETIME) AS Gate_Entry_Created_Date,
  SAFE_CAST(Gate_Entry_Shipment_Received_Date AS DATETIME) AS Gate_Entry_Shipment_Received_Date,
  Image_Url_1,
  Image_Url_2,
  Image_Url_3,
  Image_Url_4,
  Image_Url_5,
  Image_Url_6,
  Image_Url_7,
  Image_Url_8,
  Image_Url_9,
  Image_Url_10,
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  inventory_type,
  ee_extracted_at,
  row_number() over(partition by Order_Number, Order_Item_ID, Child_Sku order by ee_extracted_at DESC) as rn
FROM `shopify-pubsub-project.easycom.returns_report`
) where rn = 1
