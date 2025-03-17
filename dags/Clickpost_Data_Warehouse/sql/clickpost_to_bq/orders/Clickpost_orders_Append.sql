MERGE INTO `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Orders` AS target
USING (
  SELECT 
    `Order ID`,
    REGEXP_REPLACE(`Reference Number`, r'\.0$', '') AS `Reference Number`,
    `Order Date`,
    `Invoice Number`,
    `Invoice Date`,
    `Invoice Value`,
    `COD Value`,
    `Payment Mode`,
    `Channel Name`,
    `Account Code`,
    `Items`,
    `Items Quantity`,
    `Product SKU Code`,
    `Ingestion Date`
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY `Order ID`, REGEXP_REPLACE(`Reference Number`, r'\.0$', '') ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.orders` WHERE `Reference Number` IS NOT NULL
  )
  WHERE row_num = 1
) AS source
ON target.Order_ID = source.`Order ID` 
   AND target.Reference_Number = source.`Reference Number`
WHEN MATCHED AND source.`Ingestion Date` > target.Ingestion_Date THEN 
  UPDATE SET
    target.Order_ID = source.`Order ID`,
    target.Reference_Number = source.`Reference Number`,
    target.Order_Date = source.`Order Date`,
    target.Invoice_Number = source.`Invoice Number`,
    target.Invoice_Date = source.`Invoice Date`,
    target.Invoice_Value = source.`Invoice Value`,
    target.COD_Value = source.`COD Value`,
    target.Payment_Mode = source.`Payment Mode`,
    target.Channel_Name = source.`Channel Name`,
    target.Account_Code = source.`Account Code`,
    target.Items = source.`Items`,
    target.Items_Quantity = source.`Items Quantity`,
    target.Product_SKU_Code = source.`Product SKU Code`,
    target.Ingestion_Date = source.`Ingestion Date`
WHEN NOT MATCHED THEN 
  INSERT (
    Order_ID,
    Reference_Number,
    Order_Date,
    Invoice_Number,
    Invoice_Date,
    Invoice_Value,
    COD_Value,
    Payment_Mode,
    Channel_Name,
    Account_Code,
    Items,
    Items_Quantity,
    Product_SKU_Code,
    Ingestion_Date
  )
  VALUES (
    source.`Order ID`,
    source.`Reference Number`,
    source.`Order Date`,
    source.`Invoice Number`,
    source.`Invoice Date`,
    source.`Invoice Value`,
    source.`COD Value`,
    source.`Payment Mode`,
    source.`Channel Name`,
    source.`Account Code`,
    source.`Items`,
    source.`Items Quantity`,
    source.`Product SKU Code`,
    source.`Ingestion Date`
  );
