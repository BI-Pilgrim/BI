CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Shipping` 
PARTITION BY Ingestion_Date
AS 
SELECT 
  `Order ID` AS Order_ID,
  `Courier Partner` AS Courier_Partner,
  `AWB`,
  `RTO AWB`AS RTO_AWB,
  `Clickpost Unified Status`AS Clickpost_Unified_Status,
  `Committed SLA`AS Committed_SLA,
  `Zone`,
  `Pricing Zone`AS Pricing_Zone,
  `Shipping Cost` AS Shipping_Cost,
  `Mode of Shipment` AS Mode_of_Shipment,
  `Carrier via Aggregator`AS Carrier_via_Aggregator,
  `From Warehouse`AS From_Warehouse,
  `To Warehouse`AS To_Warehouse,
  `Box Count`AS Box_Count,
  `Ingestion Date` AS Ingestion_Date
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID`, `AWB` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.clickpost_data.shipping`
  )
WHERE row_num = 1;
