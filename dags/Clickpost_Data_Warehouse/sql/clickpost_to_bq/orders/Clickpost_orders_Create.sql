CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Orders` 
PARTITION BY Ingestion_Date
AS 
SELECT
    `Order ID` AS Order_ID, 
    `Reference Number` AS Reference_Number,
    `Order Date` AS Order_Date,
    `Invoice Number` AS Invoice_Number,
    `Invoice Date` AS Invoice_Date,
    `Invoice Value` AS Invoice_Value,
    `COD Value` AS COD_Value,
    `Payment Mode` AS Payment_Mode,
    `Channel Name` AS Channel_Name,
    `Account Code` AS Account_Code,
    `Items` AS Items,
    `Items Quantity` AS Items_Quantity,
    `Product SKU Code` AS Product_SKU_Code,
    `Ingestion Date` AS Ingestion_Date
FROM
  ( 
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID`,`Reference Number` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.clickpost_data.orders`
  )
WHERE row_num = 1;
