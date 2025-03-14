MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Returns_report` AS TARGET
USING
(
select
  distinct
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
  SAFE_CAST(CASE WHEN Total_Credit_Note_Amount = 'nan' THEN '0' ELSE Total_Credit_Note_Amount END AS FLOAT64) AS Total_Credit_Note_Amount,
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
  ee_extracted_at
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY Order_Number, Order_Item_ID, Child_Sku ORDER BY created_on) AS row_num
FROM `shopify-pubsub-project.easycom.returns_report`
WHERE DATE(created_on) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
) AS SOURCE
ON TARGET.Order_Number = SOURCE.Order_Number
and TARGET.Order_Item_ID = SOURCE.Order_Item_ID
and TARGET.Child_Sku = SOURCE.Child_Sku
WHEN MATCHED AND TARGET.created_on < SOURCE.created_on
THEN UPDATE SET
TARGET.Client_Name = SOURCE.Client_Name,
TARGET.Client_Location = SOURCE.Client_Location,
TARGET.Marketplace = SOURCE.Marketplace,
TARGET.Party = SOURCE.Party,
TARGET.Order_Number = SOURCE.Order_Number,
TARGET.Order_Item_ID = SOURCE.Order_Item_ID,
TARGET.Invoice_Number = SOURCE.Invoice_Number,
TARGET.Seller_Return_Reason = SOURCE.Seller_Return_Reason,
TARGET.Initiated_Reason = SOURCE.Initiated_Reason,
TARGET.Channel_return_reason = SOURCE.Channel_return_reason,
TARGET.Creditnote_Number = SOURCE.Creditnote_Number,
TARGET.MP_Credit_Note_No = SOURCE.MP_Credit_Note_No,
TARGET.Order_Date = SOURCE.Order_Date,
TARGET.Invoice_Date = SOURCE.Invoice_Date,
TARGET.Return_Date = SOURCE.Return_Date,
TARGET.System_Return_Entry_Date = SOURCE.System_Return_Entry_Date,
TARGET.Parent_Sku = SOURCE.Parent_Sku,
TARGET.Sku_Type = SOURCE.Sku_Type,
TARGET.Child_Sku = SOURCE.Child_Sku,
TARGET.Serial_Number = SOURCE.Serial_Number,
TARGET.Inventory_Status = SOURCE.Inventory_Status,
TARGET.Return_Quantity = SOURCE.Return_Quantity,
TARGET.companyProductId = SOURCE.companyProductId,
TARGET.Invoice_Id = SOURCE.Invoice_Id,
TARGET.Awb_Number = SOURCE.Awb_Number,
TARGET.Reverse_Carrier_Name = SOURCE.Reverse_Carrier_Name,
TARGET.Reverse_Carrier_Aggregator = SOURCE.Reverse_Carrier_Aggregator,
TARGET.Forward_Awb_Number = SOURCE.Forward_Awb_Number,
TARGET.Forward_Carrier_Name = SOURCE.Forward_Carrier_Name,
TARGET.Forward_Carrier_Aggregator = SOURCE.Forward_Carrier_Aggregator,
TARGET.Created_By = SOURCE.Created_By,
TARGET.ReturnQC_Marked_By = SOURCE.ReturnQC_Marked_By,
TARGET.RTO_Delivered_Date = SOURCE.RTO_Delivered_Date,
TARGET.MP_RefId = SOURCE.MP_RefId,
TARGET.Return_Type = SOURCE.Return_Type,
TARGET.Comments = SOURCE.Comments,
TARGET.Total_Credit_Note_Amount = SOURCE.Total_Credit_Note_Amount,
TARGET.External_Order_Code = SOURCE.External_Order_Code,
TARGET.MP_Alias = SOURCE.MP_Alias,
TARGET.MP_Delivered_Date = SOURCE.MP_Delivered_Date,
TARGET.Gate_Entry_Id = SOURCE.Gate_Entry_Id,
TARGET.Gate_Number = SOURCE.Gate_Number,
TARGET.Gate_Entry_Created_Date = SOURCE.Gate_Entry_Created_Date,
TARGET.Gate_Entry_Shipment_Received_Date = SOURCE.Gate_Entry_Shipment_Received_Date,
TARGET.Image_Url_1 = SOURCE.Image_Url_1,
TARGET.Image_Url_2 = SOURCE.Image_Url_2,
TARGET.Image_Url_3 = SOURCE.Image_Url_3,
TARGET.Image_Url_4 = SOURCE.Image_Url_4,
TARGET.Image_Url_5 = SOURCE.Image_Url_5,
TARGET.Image_Url_6 = SOURCE.Image_Url_6,
TARGET.Image_Url_7 = SOURCE.Image_Url_7,
TARGET.Image_Url_8 = SOURCE.Image_Url_8,
TARGET.Image_Url_9 = SOURCE.Image_Url_9,
TARGET.Image_Url_10 = SOURCE.Image_Url_10,
TARGET.report_id = SOURCE.report_id,
TARGET.report_type = SOURCE.report_type,
TARGET.start_date = SOURCE.start_date,
TARGET.end_date = SOURCE.end_date,
TARGET.created_on = SOURCE.created_on,
TARGET.inventory_type = SOURCE.inventory_type,
TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT
(
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
  Order_Date,
  Invoice_Date,
  Return_Date,
  System_Return_Entry_Date,
  Parent_Sku,
  Sku_Type,
  Child_Sku,
  Serial_Number,
  Inventory_Status,
  Return_Quantity,
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
  MP_RefId,
  Return_Type,
  Comments,
  Total_Credit_Note_Amount,
  External_Order_Code,
  MP_Alias,
  MP_Delivered_Date,
  Gate_Entry_Id,
  Gate_Number,
  Gate_Entry_Created_Date,
  Gate_Entry_Shipment_Received_Date,
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
  ee_extracted_at
)
VALUES
(
SOURCE.Client_Name,
SOURCE.Client_Location,
SOURCE.Marketplace,
SOURCE.Party,
SOURCE.Order_Number,
SOURCE.Order_Item_ID,
SOURCE.Invoice_Number,
SOURCE.Seller_Return_Reason,
SOURCE.Initiated_Reason,
SOURCE.Channel_return_reason,
SOURCE.Creditnote_Number,
SOURCE.MP_Credit_Note_No,
SOURCE.Order_Date,
SOURCE.Invoice_Date,
SOURCE.Return_Date,
SOURCE.System_Return_Entry_Date,
SOURCE.Parent_Sku,
SOURCE.Sku_Type,
SOURCE.Child_Sku,
SOURCE.Serial_Number,
SOURCE.Inventory_Status,
SOURCE.Return_Quantity,
SOURCE.companyProductId,
SOURCE.Invoice_Id,
SOURCE.Awb_Number,
SOURCE.Reverse_Carrier_Name,
SOURCE.Reverse_Carrier_Aggregator,
SOURCE.Forward_Awb_Number,
SOURCE.Forward_Carrier_Name,
SOURCE.Forward_Carrier_Aggregator,
SOURCE.Created_By,
SOURCE.ReturnQC_Marked_By,
SOURCE.RTO_Delivered_Date,
SOURCE.MP_RefId,
SOURCE.Return_Type,
SOURCE.Comments,
SOURCE.Total_Credit_Note_Amount,
SOURCE.External_Order_Code,
SOURCE.MP_Alias,
SOURCE.MP_Delivered_Date,
SOURCE.Gate_Entry_Id,
SOURCE.Gate_Number,
SOURCE.Gate_Entry_Created_Date,
SOURCE.Gate_Entry_Shipment_Received_Date,
SOURCE.Image_Url_1,
SOURCE.Image_Url_2,
SOURCE.Image_Url_3,
SOURCE.Image_Url_4,
SOURCE.Image_Url_5,
SOURCE.Image_Url_6,
SOURCE.Image_Url_7,
SOURCE.Image_Url_8,
SOURCE.Image_Url_9,
SOURCE.Image_Url_10,
SOURCE.report_id,
SOURCE.report_type,
SOURCE.start_date,
SOURCE.end_date,
SOURCE.created_on,
SOURCE.inventory_type,
SOURCE.ee_extracted_at
)
