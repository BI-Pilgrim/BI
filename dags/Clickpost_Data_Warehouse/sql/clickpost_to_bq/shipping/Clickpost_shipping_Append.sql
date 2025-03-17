MERGE INTO `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Shipping` AS target 
USING(
  SELECT 
      `Order ID`,
      `Courier Partner`,
      `AWB`,
      `RTO AWB`,
      `Clickpost Unified Status`,
      `Committed SLA`,
      `Zone`,
      `Pricing Zone`,
      `Shipping Cost`,
      `Mode of Shipment`,
      `Carrier via Aggregator`,
      `From Warehouse`,
      `To Warehouse`,
      `Box Count`,
      `Ingestion Date` 
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID`, `AWB` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.shipping` WHERE `Order ID` IS NOT NULL
  )
WHERE row_num = 1     
) AS source  on target.Order_ID = source.`Order ID` AND target.AWB = source.`AWB`
WHEN MATCHED AND source.`Ingestion Date` > target.Ingestion_Date THEN UPDATE SET 
target.Order_ID	               = SOURCE.`Order ID`,
target.Courier_Partner	       = SOURCE.`Courier Partner`,
target.AWB                     = SOURCE.`AWB`,
target.RTO_AWB	               = SOURCE.`RTO AWB`,
target.Clickpost_Unified_Status= SOURCE.`Clickpost Unified Status`,
target.Committed_SLA 	         = SOURCE.`Committed SLA`,
target.Zone	                   = SOURCE.`Zone`,
target.Pricing_Zone  	         = SOURCE.`Pricing Zone`,
target.Shipping_Cost 	         = SOURCE.`Shipping Cost`,
target.Mode_of_Shipment	       = SOURCE.`Mode of Shipment`,
target.Carrier_via_Aggregator  = SOURCE.`Carrier via Aggregator`,
target.From_Warehouse	         = SOURCE.`From Warehouse`,
target.To_Warehouse	           = SOURCE.`To Warehouse`,
target.Box_Count               = SOURCE.`Box Count`,
target.Ingestion_Date	        = SOURCE.`Ingestion Date` 
WHEN NOT MATCHED THEN INSERT ( 
Order_ID,
Courier_Partner,
AWB,
RTO_AWB,
Clickpost_Unified_Status,
Committed_SLA,
Zone,
Pricing_Zone,
Shipping_Cost,
Mode_of_Shipment,
Carrier_via_Aggregator,
From_Warehouse,
To_Warehouse,
Box_Count,
Ingestion_Date
) 
VALUES(
`Order ID`,
`Courier Partner`,
`AWB`,
`RTO AWB`,
`Clickpost Unified Status`,
`Committed SLA`,
`Zone`,
`Pricing Zone`,
`Shipping Cost`,
`Mode of Shipment`,
`Carrier via Aggregator`,
`From Warehouse`,
`To Warehouse`,
`Box Count`,
`Ingestion Date`
)
