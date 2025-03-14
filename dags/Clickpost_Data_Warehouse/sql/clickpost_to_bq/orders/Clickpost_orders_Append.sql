MERGE INTO `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Orders` AS target 
USING(
  SELECT 
`Order ID`,
`Reference Number`,
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
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID`,`Reference Number` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.clickpost_data.orders`
  )
WHERE row_num = 1
) AS source  on target.Order_ID = source.`Order ID` AND target.Reference_Number = source.`Reference Number`
WHEN MATCHED AND source.`Ingestion Date` > target.Ingestion_Date THEN UPDATE SET
target.Order_ID					  =   SOURCE.`Order ID`,
target.Reference_Number		=		SOURCE.`Reference Number`,
target.Order_Date					=   SOURCE.`Order Date`,
target.Invoice_Number			=		SOURCE.`Invoice Number`,
target.Invoice_Date				=	  SOURCE.`Invoice Date`,
target.Invoice_Value			=		SOURCE.`Invoice Value`,
target.COD_Value					=   SOURCE.`COD Value`,
target.Payment_Mode				=	  SOURCE.`Payment Mode`,
target.Channel_Name				=	  SOURCE.`Channel Name`,
target.Account_Code				=	  SOURCE.`Account Code`,
target.Items					    =   SOURCE.`Items`,
target.Items_Quantity			=		SOURCE.`Items Quantity`,
target.Product_SKU_Code		=		SOURCE.`Product SKU Code`,
target.Ingestion_Date			=		SOURCE.`Ingestion Date` 
WHEN NOT MATCHED THEN INSERT ( 
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
VALUES(
`Order ID`,
`Reference Number`,
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
)