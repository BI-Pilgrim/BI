MERGE INTO `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Package` AS target 
USING(
  SELECT 
      `Order ID`,
      `Shipment Length`,
      `Shipment Breadth`,
      `Shipment Height`,
      `Shipment Weight`,
      `Item Length`,
      `Item Breadth`,
      `Item Height`,
      `Actual Weight`,
      `Chargeable Weight`,
      `Volumetric Weight`,
      `SKU`,
      `SKU Wise Item Quantity`,
      `Return Product URL`,
      `Ingestion Date`
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID`,`SKU` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.clickpost_data.package`
  )
WHERE row_num = 1
) AS source  on target.Order_ID = source.`Order ID` AND target.SKU = source.`SKU`
WHEN MATCHED AND source.`Ingestion Date` > target.Ingestion_Date THEN UPDATE SET 
target.Order_ID								  =     SOURCE.`Order ID`,
target.Shipment_Length					=			SOURCE.`Shipment Length`,
target.Shipment_Breadth					=			SOURCE.`Shipment Breadth`,
target.Shipment_Height					=			SOURCE.`Shipment Height`,
target.Shipment_Weight					=			SOURCE.`Shipment Weight`,
target.Item_Length							=	    SOURCE.`Item Length`,
target.Item_Breadth							=	    SOURCE.`Item Breadth`,
target.Item_Height							=	    SOURCE.`Item Height`,
target.Actual_Weight						=		  SOURCE.`Actual Weight`,
target.Chargeable_Weight				=		  SOURCE.`Chargeable Weight`,
target.Volumetric_Weight				=			SOURCE.`Volumetric Weight`,
target.SKU								      =     SOURCE.`SKU`,
target.SKU_Wise_Item_Quantity		=			SOURCE.`SKU Wise Item Quantity`,
target.Return_Product_URL				=			SOURCE.`Return Product URL`,
target.Ingestion_Date						=		  SOURCE.`Ingestion Date`
WHEN NOT MATCHED THEN INSERT (
Order_ID, 
Shipment_Length,
Shipment_Breadth,
Shipment_Height,
Shipment_Weight,
Item_Length,
Item_Breadth,
Item_Height,
Actual_Weight,
Chargeable_Weight,
Volumetric_Weight,
SKU,
SKU_Wise_Item_Quantity,
Return_Product_URL,
Ingestion_Date
) 
VALUES( 
`Order ID`,
`Shipment Length`,
`Shipment Breadth`,
`Shipment Height`,
`Shipment Weight`,
`Item Length`,
`Item Breadth`,
`Item Height`,
`Actual Weight`,
`Chargeable Weight`,
`Volumetric Weight`,
`SKU`,
`SKU Wise Item Quantity`,
`Return Product URL`,
`Ingestion Date`
)