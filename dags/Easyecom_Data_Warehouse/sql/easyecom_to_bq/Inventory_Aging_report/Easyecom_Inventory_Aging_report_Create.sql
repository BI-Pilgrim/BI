
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Inventory_Aging_report`
PARTITION BY DATE_TRUNC(created_on,day)
-- CLUSTER BY 
OPTIONS(
 description = "All Return Order table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


select
  Location,
  Company,
  sku,
  Name,
  EAN,
  HSN,
  Description,
  SAFE_CAST(D30 AS FLOAT64 ) AS D30,
  SAFE_CAST(D60 AS FLOAT64 ) AS D60,
  SAFE_CAST(D90 AS FLOAT64 ) AS D90,
  SAFE_CAST(D120 AS FLOAT64 ) AS D120,
  SAFE_CAST(D180 AS FLOAT64 ) AS D180,
  SAFE_CAST(D240 AS FLOAT64 ) AS D240,
  SAFE_CAST(D300 AS FLOAT64 ) AS D300,
  SAFE_CAST(D365 AS FLOAT64 ) AS D365,
  SAFE_CAST(D365Above AS FLOAT64 ) AS D365Above,
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  inventory_type,
  -- ee_extracted_at
  FROM `shopify-pubsub-project.easycom.inventory_aging_report`