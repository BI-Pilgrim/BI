<<<<<<< Updated upstream
MERGE INTO `` AS TARGET
=======
MERGE INTO `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.GRN_Details_Reports` AS TARGET
>>>>>>> Stashed changes
USING
(
SELECT
  warehouseLocation,
  poNo,
<<<<<<< Updated upstream
  CAST(poDate AS DATETIME) AS poDate,
  CAST(Mrp AS FLOAT64) AS Mrp,
  poRef,
  grnNo,
  CAST(grnDetailsDate AS DATETIME) AS grnDetailsDate,
  vendorName,
  vendorInvoiceNo,
  CAST(vendorInvoiceDate AS DATETIME) AS vendorInvoiceDate,
=======
  CAST(SAFE_CAST(poDate AS DATETIME) AS DATETIME) AS poDate,
  SAFE_CAST(Mrp AS FLOAT64) AS Mrp,
  poRef,
  grnNo,
  SAFE_CAST(grnDetailsDate AS DATETIME) AS grnDetailsDate,
  vendorName,
  vendorInvoiceNo,
  SAFE_CAST(vendorInvoiceDate AS DATETIME) AS vendorInvoiceDate,
>>>>>>> Stashed changes
  productName,
  companyProductId,
  sku,
  ean,
<<<<<<< Updated upstream
  CAST(unitPrice AS FLOAT64) AS unitPrice,
  CAST(taxableAmount AS FLOAT64) AS taxableAmount,
  poStatus,
  CAST(poQty AS FLOAT64) AS poQty,
  CAST(receivedQty AS FLOAT64) AS receivedQty,
  CAST(receivedValue AS FLOAT64) AS receivedValue,
  userName,
  batchCode,
  CAST(exipryDate AS DATETIME) AS exipryDate,
=======
  SAFE_CAST(unitPrice AS FLOAT64) AS unitPrice,
  SAFE_CAST(taxableAmount AS FLOAT64) AS taxableAmount,
  poStatus,
  SAFE_CAST(poQty AS FLOAT64) AS poQty,
  SAFE_CAST(receivedQty AS FLOAT64) AS receivedQty,
  SAFE_CAST(receivedValue AS FLOAT64) AS receivedValue,
  userName,
  batchCode,
  SAFE_CAST(exipryDate AS DATETIME) AS exipryDate,
>>>>>>> Stashed changes
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
  inventory_type,
  ee_extracted_at
FROM
(
SELECT
*,
<<<<<<< Updated upstream
ROW_NUMBER() OVER(PARTITON BY ORDER BY _airbyte_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.grn_details_report`
WHERE DATE(_airbyte_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE. = TARGET.
WHEN MATCHED AND TARGET.ee_extracted_at < SOURCE.ee_extracted_at
THEN UPDATE SET
TARGET.warehouseLocation = SOURCE.warehouseLocation,
TARGET.poNo = SOURCE.poNo,
TARGET.CAST(poDate AS DATETIME) AS poDate = SOURCE.CAST(poDate AS DATETIME) AS poDate,
TARGET.CAST(Mrp AS FLOAT64) AS Mrp = SOURCE.CAST(Mrp AS FLOAT64) AS Mrp,
TARGET.poRef = SOURCE.poRef,
TARGET.grnNo = SOURCE.grnNo,
TARGET.CAST(grnDetailsDate AS DATETIME) AS grnDetailsDate = SOURCE.CAST(grnDetailsDate AS DATETIME) AS grnDetailsDate,
TARGET.vendorName = SOURCE.vendorName,
TARGET.vendorInvoiceNo = SOURCE.vendorInvoiceNo,
TARGET.CAST(vendorInvoiceDate AS DATETIME) AS vendorInvoiceDate = SOURCE.CAST(vendorInvoiceDate AS DATETIME) AS vendorInvoiceDate,
=======
ROW_NUMBER() OVER(PARTITiON BY ee_extracted_at ORDER BY ee_extracted_at) as row_num
FROM `shopify-pubsub-project.easycom.grn_details_report`
WHERE DATE(ee_extracted_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 10 DAY)
)
WHERE row_num = 1 -- Keep only the most recent row per customer_id and segments_date
) AS SOURCE
ON SOURCE.poNo = TARGET.poNo AND SOURCE.companyProductId = TARGET.companyProductId
WHEN MATCHED AND TARGET.grnDetailsDate < SOURCE.grnDetailsDate
THEN UPDATE SET
TARGET.warehouseLocation = SOURCE.warehouseLocation,
TARGET.poNo = SOURCE.poNo,
TARGET.poDate = SOURCE. poDate,
TARGET.Mrp = SOURCE.Mrp,
TARGET.poRef = SOURCE.poRef,
TARGET.grnNo = SOURCE.grnNo,
TARGET.grnDetailsDate = SOURCE.grnDetailsDate,
TARGET.vendorName = SOURCE.vendorName,
TARGET.vendorInvoiceNo = SOURCE.vendorInvoiceNo,
TARGET.vendorInvoiceDate = SOURCE.vendorInvoiceDate,
>>>>>>> Stashed changes
TARGET.productName = SOURCE.productName,
TARGET.companyProductId = SOURCE.companyProductId,
TARGET.sku = SOURCE.sku,
TARGET.ean = SOURCE.ean,
<<<<<<< Updated upstream
TARGET.CAST(unitPrice AS FLOAT64) AS unitPrice = SOURCE.CAST(unitPrice AS FLOAT64) AS unitPrice,
TARGET.CAST(taxableAmount AS FLOAT64) AS taxableAmount = SOURCE.CAST(taxableAmount AS FLOAT64) AS taxableAmount,
TARGET.poStatus = SOURCE.poStatus,
TARGET.CAST(poQty AS FLOAT64) AS poQty = SOURCE.CAST(poQty AS FLOAT64) AS poQty,
TARGET.CAST(receivedQty AS FLOAT64) AS receivedQty = SOURCE.CAST(receivedQty AS FLOAT64) AS receivedQty,
TARGET.CAST(receivedValue AS FLOAT64) AS receivedValue = SOURCE.CAST(receivedValue AS FLOAT64) AS receivedValue,
TARGET.userName = SOURCE.userName,
TARGET.batchCode = SOURCE.batchCode,
TARGET.CAST(exipryDate AS DATETIME) AS exipryDate = SOURCE.CAST(exipryDate AS DATETIME) AS exipryDate,
=======
TARGET.unitPrice = SOURCE.unitPrice,
TARGET.taxableAmount = SOURCE.taxableAmount,
TARGET.poStatus = SOURCE.poStatus,
TARGET.poQty = SOURCE.poQty,
TARGET.receivedQty = SOURCE.receivedQty,
TARGET.receivedValue = SOURCE.receivedValue,
TARGET.userName = SOURCE.userName,
TARGET.batchCode = SOURCE.batchCode,
TARGET.exipryDate = SOURCE.exipryDate,
>>>>>>> Stashed changes
TARGET.report_id = SOURCE.report_id,
TARGET.report_type = SOURCE.report_type,
TARGET.start_date = SOURCE.start_date,
TARGET.end_date = SOURCE.end_date,
TARGET.created_on = SOURCE.created_on,
<<<<<<< Updated upstream
TARGET.inventory_type = SOURCE.inventory_type,
TARGET.ee_extracted_a = SOURCE.ee_extracted_at
=======
TARGET.inventory_type = SOURCE.inventory_type
>>>>>>> Stashed changes
WHEN NOT MATCHED
THEN INSERT
(
  warehouseLocation,
  poNo,
  poDate,
  Mrp,
  poRef,
  grnNo,
  grnDetailsDate,
  vendorName,
  vendorInvoiceNo,
  vendorInvoiceDate,
  productName,
  companyProductId,
  sku,
  ean,
  unitPrice,
  taxableAmount,
  poStatus,
  poQty,
  receivedQty,
  receivedValue,
  userName,
  batchCode,
  exipryDate,
  report_id,
  report_type,
  start_date,
  end_date,
  created_on,
<<<<<<< Updated upstream
  inventory_type,
  ee_extracted_at
=======
  inventory_type
>>>>>>> Stashed changes
)
VALUES
(
SOURCE.warehouseLocation,
SOURCE.poNo,
<<<<<<< Updated upstream
SOURCE.CAST(poDate AS DATETIME) AS poDate,
SOURCE.CAST(Mrp AS FLOAT64) AS Mrp,
SOURCE.poRef,
SOURCE.grnNo,
SOURCE.CAST(grnDetailsDate AS DATETIME) AS grnDetailsDate,
SOURCE.vendorName,
SOURCE.vendorInvoiceNo,
SOURCE.CAST(vendorInvoiceDate AS DATETIME) AS vendorInvoiceDate,
=======
SOURCE.poDate,
SOURCE.Mrp,
SOURCE.poRef,
SOURCE.grnNo,
SOURCE.grnDetailsDate,
SOURCE.vendorName,
SOURCE.vendorInvoiceNo,
SOURCE.vendorInvoiceDate,
>>>>>>> Stashed changes
SOURCE.productName,
SOURCE.companyProductId,
SOURCE.sku,
SOURCE.ean,
<<<<<<< Updated upstream
SOURCE.CAST(unitPrice AS FLOAT64) AS unitPrice,
SOURCE.CAST(taxableAmount AS FLOAT64) AS taxableAmount,
SOURCE.poStatus,
SOURCE.CAST(poQty AS FLOAT64) AS poQty,
SOURCE.CAST(receivedQty AS FLOAT64) AS receivedQty,
SOURCE.CAST(receivedValue AS FLOAT64) AS receivedValue,
SOURCE.userName,
SOURCE.batchCode,
SOURCE.CAST(exipryDate AS DATETIME) AS exipryDate,
=======
SOURCE.unitPrice,
SOURCE.taxableAmount,
SOURCE.poStatus,
SOURCE.poQty,
SOURCE.receivedQty,
SOURCE.receivedValue,
SOURCE.userName,
SOURCE.batchCode,
SOURCE.exipryDate,
>>>>>>> Stashed changes
SOURCE.report_id,
SOURCE.report_type,
SOURCE.start_date,
SOURCE.end_date,
SOURCE.created_on,
<<<<<<< Updated upstream
SOURCE.inventory_type,
SOURCE.ee_extracted_a

=======
SOURCE.inventory_type
>>>>>>> Stashed changes
)
