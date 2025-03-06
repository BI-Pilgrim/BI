CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Tracking` 
PARTITION BY Ingestion_Date
AS 
SELECT 
    `Order ID` AS Order_ID,
    `Created at` AS Created_at,
    `Pickup Date` AS Pickup_Date,
    `Current Location` AS Current_Location,
    `Latest Timestamp` AS Latest_Timestamp,
    `Latest Remark` AS Latest_Remark,
    `Delivery Date` AS Delivery_Date,
    `Updated at` AS Updated_at,
    `Expected delivery date by Courier Partner` AS Expected_delivery_date_by_Courier_Partner,
    `Expected Date Of Delivery Min` AS Expected_Date_Of_Delivery_Min,
    `Expected Date Of Delivery Max` AS Expected_Date_Of_Delivery_Max,
    `Out For Delivery 1st Attempt` AS Out_For_Delivery_1st_Attempt,
    `Out For Delivery 2nd Attempt` AS Out_For_Delivery_2nd_Attempt,
    `Out For Delivery 3rd Attempt` AS Out_For_Delivery_3rd_Attempt,
    `Out For Delivery 4th Attempt` AS Out_For_Delivery_4th_Attempt,
    `Out For Delivery 5th Attempt` AS Out_For_Delivery_5th_Attempt,
    `Out For Delivery Attempts` AS Out_For_Delivery_Attempts,
    `Reason For Last Failed Delivery` AS Reason_For_Last_Failed_Delivery,
    `TimeStamp Of Last Failed Delivery` AS TimeStamp_Of_Last_Failed_Delivery,
    `Remark Of Last Failed Delivery` AS Remark_Of_Last_Failed_Delivery,
    `Is shipment stuck in lifecycle` AS Is_shipment_stuck_in_lifecycle,
    `RTO Mark Date` AS RTO_Mark_Date,
    `Out For Pickup 1st Attempt` AS Out_For_Pickup_1st_Attempt,
    `Out For Pickup 2nd Attempt` AS Out_For_Pickup_2nd_Attempt,
    `Out For Pickup 3rd Attempt` AS Out_For_Pickup_3rd_Attempt,
    `Out For Pickup Attempts` AS Out_For_Pickup_Attempts,
    `Destination Hub In Scan Time` AS Destination_Hub_In_Scan_Time,
    `Origin Hub In Timestamp` AS Origin_Hub_In_Timestamp,
    `Origin Hub Out Timestamp` AS Origin_Hub_Out_Timestamp,
    `RTO Intransit Timestamp` AS RTO_Intransit_Timestamp,
    `RTO Delivery Date` AS RTO_Delivery_Date,
    `Original Appointment Start Time` AS Original_Appointment_Start_Time,
    `Original Appointment End Time` AS Original_Appointment_End_Time,
    `Current Appointment Start Time` AS Current_Appointment_Start_Time,
    `Current Appointment End Time` AS Current_Appointment_End_Time,
    `Ingestion Date` AS Ingestion_Date 
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.clickpost_data.tracking`
  )
WHERE row_num = 1;
