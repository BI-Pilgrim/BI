MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Pending_Returns_report` AS TARGET
<<<<<<< Updated upstream
USING
(
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
FROM
(
SELECT
*,
ROW_NUMBER() OVER(PARTITTION BY  ORDER BY ) AS row_num
FROM `shopify-pubsub-project.easycom.pending_returns_report`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1
) AS SOURCE
ON TARGET. = SOURCE.
WHEN MATCHED AND TARGET. > SOURCE.
THEN UPDATE SET
TARGET.Client_Name = SOURCE.Client_Name,
TARGET.Client_Location = SOURCE.Client_Location,
TARGET.Marketplace = SOURCE.Marketplace,
TARGET.Party = SOURCE.Party,
TARGET.Order_Number = SOURCE.Order_Number,
TARGET.Order_Item_ID = SOURCE.Order_Item_ID,
TARGET.Invoice_Number = SOURCE.Invoice_Number,
TARGET.Channel_return_reason = SOURCE.Channel_return_reason,
TARGET.Order_Date = SOURCE.Order_Date,
TARGET.Invoice_Date = SOURCE.Invoice_Date,
TARGET.Return_Initiated_Date = SOURCE.Return_Initiated_Date,
TARGET.MP_Delivered_Date = SOURCE.MP_Delivered_Date,
TARGET.sku = SOURCE.sku,
TARGET.Return_Quantity = SOURCE.Return_Quantity,
TARGET.companyProductId = SOURCE.companyProductId,
TARGET.Return_Status = SOURCE.Return_Status,
TARGET.Return_Type = SOURCE.Return_Type,
TARGET.Invoice_Id = SOURCE.Invoice_Id,
TARGET.Myntra_Packet_Order_Id = SOURCE.Myntra_Packet_Order_Id,
TARGET.Packet_Amount = SOURCE.Packet_Amount,
TARGET.Reverse_AWB_Number = SOURCE.Reverse_AWB_Number,
TARGET.Reverse_Carrier_Name = SOURCE.Reverse_Carrier_Name,
TARGET.Reverse_Carrier_Aggregator = SOURCE.Reverse_Carrier_Aggregator,
TARGET.Forward_Awb_Number = SOURCE.Forward_Awb_Number,
TARGET.Forward_Carrier_Name = SOURCE.Forward_Carrier_Name,
TARGET.Forward_Carrier_Aggregator = SOURCE.Forward_Carrier_Aggregator,
TARGET.Created_By = SOURCE.Created_By,
TARGET.MP_RefId = SOURCE.MP_RefId,
TARGET.External_Order_Code = SOURCE.External_Order_Code,
TARGET.MP_Alias = SOURCE.MP_Alias,
TARGET.Gate_Entry_Id = SOURCE.Gate_Entry_Id,
TARGET.Gate_Number = SOURCE.Gate_Number,
TARGET.Gate_Entry_Created_Date = SOURCE.Gate_Entry_Created_Date,
TARGET.Gate_Entry_Shipment_Received_Date = SOURCE.Gate_Entry_Shipment_Received_Date,
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
=======
USING (
  SELECT
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
    SAFE_CAST(Return_Quantity AS FLOAT64) AS Return_Quantity,
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
    ee_extracted_at
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY Order_Number, Order_Item_ID 
        ORDER BY ee_extracted_at DESC
      ) AS row_num
    FROM `shopify-pubsub-project.easycom.pending_returns_report`
    WHERE DATE(end_date) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
  )
  WHERE row_num = 1
) AS SOURCE
ON TARGET.Order_Number = SOURCE.Order_Number 
   AND TARGET.Order_Item_ID = SOURCE.Order_Item_ID
WHEN MATCHED AND TARGET.end_date < SOURCE.end_date
THEN UPDATE SET
  TARGET.Client_Name = SOURCE.Client_Name,
  TARGET.Client_Location = SOURCE.Client_Location,
  TARGET.Marketplace = SOURCE.Marketplace,
  TARGET.Party = SOURCE.Party,
  TARGET.Order_Number = SOURCE.Order_Number,
  TARGET.Order_Item_ID = SOURCE.Order_Item_ID,
  TARGET.Invoice_Number = SOURCE.Invoice_Number,
  TARGET.Channel_return_reason = SOURCE.Channel_return_reason,
  TARGET.Order_Date = SOURCE.Order_Date,
  TARGET.Invoice_Date = SOURCE.Invoice_Date,
  TARGET.Return_Initiated_Date = SOURCE.Return_Initiated_Date,
  TARGET.MP_Delivered_Date = SOURCE.MP_Delivered_Date,
  TARGET.sku = SOURCE.sku,
  TARGET.Return_Quantity = SOURCE.Return_Quantity,
  TARGET.companyProductId = SOURCE.companyProductId,
  TARGET.Return_Status = SOURCE.Return_Status,
  TARGET.Return_Type = SOURCE.Return_Type,
  TARGET.Invoice_Id = SOURCE.Invoice_Id,
  TARGET.Myntra_Packet_Order_Id = SOURCE.Myntra_Packet_Order_Id,
  TARGET.Packet_Amount = SOURCE.Packet_Amount,
  TARGET.Reverse_AWB_Number = SOURCE.Reverse_AWB_Number,
  TARGET.Reverse_Carrier_Name = SOURCE.Reverse_Carrier_Name,
  TARGET.Reverse_Carrier_Aggregator = SOURCE.Reverse_Carrier_Aggregator,
  TARGET.Forward_Awb_Number = SOURCE.Forward_Awb_Number,
  TARGET.Forward_Carrier_Name = SOURCE.Forward_Carrier_Name,
  TARGET.Forward_Carrier_Aggregator = SOURCE.Forward_Carrier_Aggregator,
  TARGET.Created_By = SOURCE.Created_By,
  TARGET.MP_RefId = SOURCE.MP_RefId,
  TARGET.External_Order_Code = SOURCE.External_Order_Code,
  TARGET.MP_Alias = SOURCE.MP_Alias,
  TARGET.Gate_Entry_Id = SOURCE.Gate_Entry_Id,
  TARGET.Gate_Number = SOURCE.Gate_Number,
  TARGET.Gate_Entry_Created_Date = SOURCE.Gate_Entry_Created_Date,
  TARGET.Gate_Entry_Shipment_Received_Date = SOURCE.Gate_Entry_Shipment_Received_Date,
  TARGET.report_id = SOURCE.report_id,
  TARGET.report_type = SOURCE.report_type,
  TARGET.start_date = SOURCE.start_date,
  TARGET.end_date = SOURCE.end_date,
  TARGET.created_on = SOURCE.created_on,
  TARGET.inventory_type = SOURCE.inventory_type,
  TARGET.ee_extracted_at = SOURCE.ee_extracted_at
WHEN NOT MATCHED
THEN INSERT (
>>>>>>> Stashed changes
  Client_Name,
  Client_Location,
  Marketplace,
  Party,
  Order_Number,
  Order_Item_ID,
  Invoice_Number,
  Channel_return_reason,
  Order_Date,
  Invoice_Date,
  Return_Initiated_Date,
  MP_Delivered_Date,
  sku,
  Return_Quantity,
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
<<<<<<< Updated upstream
  ee_extracted_at,
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
SOURCE.Channel_return_reason,
SOURCE.Order_Date,
SOURCE.Invoice_Date,
SOURCE.Return_Initiated_Date,
SOURCE.MP_Delivered_Date,
SOURCE.sku,
SOURCE.Return_Quantity,
SOURCE.companyProductId,
SOURCE.Return_Status,
SOURCE.Return_Type,
SOURCE.Invoice_Id,
SOURCE.Myntra_Packet_Order_Id,
SOURCE.Packet_Amount,
SOURCE.Reverse_AWB_Number,
SOURCE.Reverse_Carrier_Name,
SOURCE.Reverse_Carrier_Aggregator,
SOURCE.Forward_Awb_Number,
SOURCE.Forward_Carrier_Name,
SOURCE.Forward_Carrier_Aggregator,
SOURCE.Created_By,
SOURCE.MP_RefId,
SOURCE.External_Order_Code,
SOURCE.MP_Alias,
SOURCE.Gate_Entry_Id,
SOURCE.Gate_Number,
SOURCE.Gate_Entry_Created_Date,
SOURCE.Gate_Entry_Shipment_Received_Date,
SOURCE.report_id,
SOURCE.report_type,
SOURCE.start_date,
SOURCE.end_date,
SOURCE.created_on,
SOURCE.inventory_type,
SOURCE.ee_extracted_at
)
=======
  ee_extracted_at
)
VALUES (
  SOURCE.Client_Name,
  SOURCE.Client_Location,
  SOURCE.Marketplace,
  SOURCE.Party,
  SOURCE.Order_Number,
  SOURCE.Order_Item_ID,
  SOURCE.Invoice_Number,
  SOURCE.Channel_return_reason,
  SOURCE.Order_Date,
  SOURCE.Invoice_Date,
  SOURCE.Return_Initiated_Date,
  SOURCE.MP_Delivered_Date,
  SOURCE.sku,
  SOURCE.Return_Quantity,
  SOURCE.companyProductId,
  SOURCE.Return_Status,
  SOURCE.Return_Type,
  SOURCE.Invoice_Id,
  SOURCE.Myntra_Packet_Order_Id,
  SOURCE.Packet_Amount,
  SOURCE.Reverse_AWB_Number,
  SOURCE.Reverse_Carrier_Name,
  SOURCE.Reverse_Carrier_Aggregator,
  SOURCE.Forward_Awb_Number,
  SOURCE.Forward_Carrier_Name,
  SOURCE.Forward_Carrier_Aggregator,
  SOURCE.Created_By,
  SOURCE.MP_RefId,
  SOURCE.External_Order_Code,
  SOURCE.MP_Alias,
  SOURCE.Gate_Entry_Id,
  SOURCE.Gate_Number,
  SOURCE.Gate_Entry_Created_Date,
  SOURCE.Gate_Entry_Shipment_Received_Date,
  SOURCE.report_id,
  SOURCE.report_type,
  SOURCE.start_date,
  SOURCE.end_date,
  SOURCE.created_on,
  SOURCE.inventory_type,
  SOURCE.ee_extracted_at
);
>>>>>>> Stashed changes