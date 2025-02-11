create or replace table `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Status_Wise_Stock_report`
as
select distinct
  SAFE_CAST(Report_Generated_Date AS DATETIME) AS Report_Generated_Date,
  Company_Token,
  Location,
  Product_Name,
  Description,
  SKU,
  EAN,
  Brand,
  SAFE_CAST(Weight_gm_ AS FLOAT64) AS Weight_gm_,
  SAFE_CAST(Length_cm_ AS FLOAT64) AS Length_cm_,
  SAFE_CAST(Height_cm_ AS FLOAT64) AS Height_cm_,
  SAFE_CAST(Width_cm_ AS FLOAT64) AS Width_cm_,
  SAFE_CAST(Received AS FLOAT64) AS Received,
  SAFE_CAST(Reserved_Not_Picked_ AS FLOAT64) AS Reserved_Not_Picked_,
  SAFE_CAST(Reserved_Picked_ AS FLOAT64) AS Reserved_Picked_,
  SAFE_CAST(Damaged AS FLOAT64) AS Damaged,
  SAFE_CAST(Discard_Fraud AS FLOAT64) AS Discard_Fraud,
  SAFE_CAST(Repair AS FLOAT64) AS Repair,
  SAFE_CAST(To_Receive AS FLOAT64) AS To_Receive,
  SAFE_CAST(Return_Available AS FLOAT64) AS Return_Available,
  SAFE_CAST(Available_Quantity AS FLOAT64) AS Available_Quantity,
  SAFE_CAST(Available_Quantity_Bin_Locked_ AS FLOAT64) AS Available_Quantity_Bin_Locked_,
  SAFE_CAST(Quarantine AS FLOAT64) AS Quarantine,
  SAFE_CAST(Marketplace_Available AS FLOAT64) AS Marketplace_Available,
  SAFE_CAST(Undispatched_Unassigned_Quantity AS FLOAT64) AS Undispatched_Unassigned_Quantity,
  SAFE_CAST(QC_Passed AS FLOAT64) AS QC_Passed,
  SAFE_CAST(QC_Failed AS FLOAT64) AS QC_Failed,
  SAFE_CAST(QC_Pending AS FLOAT64) AS QC_Pending,
  SAFE_CAST(NearExpiry AS FLOAT64) AS NearExpiry,
  SAFE_CAST(Expiry AS FLOAT64) AS Expiry,
  SAFE_CAST(Total_Lost AS FLOAT64) AS Total_Lost,
  SAFE_CAST(Questionable AS FLOAT64) AS Questionable,
  SAFE_CAST(Website_Inventory AS FLOAT64) AS Website_Inventory,
  SAFE_CAST(E_Com_Inventory AS FLOAT64) AS E_Com_Inventory,
  SAFE_CAST(Retail_Inventory AS FLOAT64) AS Retail_Inventory,
  SAFE_CAST(IIA_Inventory AS FLOAT64) AS IIA_Inventory,
  SAFE_CAST(Amazon_Reserved AS FLOAT64) AS Amazon_Reserved,
  SAFE_CAST(Lost_In_Cycle_Count AS FLOAT64) AS Lost_In_Cycle_Count,
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
ROW_NUMBER() OVER(PARTITION BY SKU,Company_Token ORDER BY ee_extracted_at DESC) AS row_num
FROM `shopify-pubsub-project.easycom.status_wise_stock_report`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1