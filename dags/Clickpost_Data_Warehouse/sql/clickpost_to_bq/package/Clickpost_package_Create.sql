CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Package` 
PARTITION BY Ingestion_Date
AS 
SELECT 
    `Order ID` AS Order_ID,
    `Shipment Length` AS Shipment_Length,
    `Shipment Breadth` AS Shipment_Breadth,
    `Shipment Height` AS Shipment_Height,
    `Shipment Weight` AS Shipment_Weight,
    `Item Length` AS Item_Length,
    `Item Breadth` AS Item_Breadth,
    `Item Height` AS Item_Height,
    `Actual Weight` AS Actual_Weight,
    `Chargeable Weight` AS Chargeable_Weight,
    `Volumetric Weight` AS Volumetric_Weight,
    `SKU` AS SKU,
    `SKU Wise Item Quantity` AS SKU_Wise_Item_Quantity,
    `Return Product URL` AS Return_Product_URL,
    `Ingestion Date` AS Ingestion_Date
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID`,`SKU` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.clickpost_data.package`
  )
WHERE row_num = 1;