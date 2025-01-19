CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.GRN_Details_Reports`
PARTITION BY DATE_TRUNC(poDate,day)
-- CLUSTER BY 
OPTIONS(
 description = "GRN Details Report table is partitioned on order date at day level",
 require_partition_filter = False
 )
 AS


  select
  
  warehouseLocation,
  poNo,
  CAST(poDate AS DATETIME) AS poDate,
  CAST(Mrp AS FLOAT64) AS Mrp,
  poRef,
  grnNo,
  CAST(grnDetailsDate AS DATETIME) AS grnDetailsDate,
  vendorName,
  vendorInvoiceNo,
  CAST(vendorInvoiceDate AS DATETIME) AS vendorInvoiceDate,
  productName,
  companyProductId,
  sku,
  ean,
  CAST(unitPrice AS FLOAT64) AS unitPrice,
  CAST(taxableAmount AS FLOAT64) AS taxableAmount,
  poStatus,
  CAST(poQty AS FLOAT64) AS poQty,
  CAST(receivedQty AS FLOAT64) AS receivedQty,
  CAST(receivedValue AS FLOAT64) AS receivedValue,
  userName,
  batchCode,
  CAST(exipryDate AS DATETIME) AS exipryDate,
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  inventory_type,
  -- ee_extracted_at
  FROM `shopify-pubsub-project.easycom.grn_details_report`

