CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Mini_Sales_report`
PARTITION BY start_date
-- CLUSTER BY 
-- OPTIONS(
--  description = "Mini Sales Report table is partitioned on order date at day level"
--  require_partition_filter = False
--  )
 AS
select distinct
  Client_Location,
  Seller_GST_Num,
  MP_Name,
  B2B_Sales_Channel,
  Order_Number,
  Suborder_No,
  Order_Type,
  Manifest_ID,
  EE_Invoice_No,
  MP_Ref_No_MP_Invoice_Number,
  Order_Status,
  Shipping_Status,
  CASE WHEN Import_Date = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Import_Date as DATETIME) AS DATE) END AS Import_Date,
  CASE WHEN Order_Date = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Order_Date as DATETIME) AS DATE) END AS Order_Date,
  CASE WHEN Assigned_At = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Assigned_At as DATETIME) AS DATE) END AS Assigned_At,
  CASE WHEN TAT = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(TAT as DATETIME) AS DATE) END AS TAT,
  CASE WHEN Invoice_Date = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Invoice_Date as DATETIME) AS DATE) END AS Invoice_Date,
  CASE WHEN QC_Confirmed_At = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(QC_Confirmed_At as DATETIME) AS DATE) END AS QC_Confirmed_At,
  CASE WHEN Confirmed_At = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Confirmed_At as DATETIME) AS DATE) END AS Confirmed_At,
  CASE WHEN Printed_At = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Printed_At as DATETIME) AS DATE) END AS Printed_At,
  CASE WHEN Manifested_At = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Manifested_At as DATETIME) AS DATE) END AS Manifested_At,
  CASE WHEN Cancelled_At = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Cancelled_At as DATETIME) AS DATE) END AS Cancelled_At,
  CASE WHEN Delivered_At = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Delivered_At as DATETIME) AS DATE) END AS Delivered_At,
  CASE WHEN Handover_At = 'nan' THEN DATE('0001-01-01') ELSE CAST(CAST(Handover_At as DATETIME) AS DATE) END AS Handover_At,
  Batch_ID,
  Message,
  Courier_Aggregator_Name,
  Courier_Name,
  Tracking_Number,
  Packing_Material_SKU,
  CASE WHEN Suborder_Quantity IS NULL THEN 0.0 ELSE SAFE_CAST(Suborder_Quantity AS FLOAT64) END AS Suborder_Quantity,
  CASE WHEN Item_Quantity IS NULL THEN 0.0 ELSE SAFE_CAST(Item_Quantity AS FLOAT64) END AS Item_Quantity,
  SKU,
  SKU_Type,
  SAFE_CAST(Sub_Product_Count AS FLOAT64) AS Sub_Product_Count,
  Marketplace_Sku,
  Product_Name,
  Category,
  Brand,
  Model_No,
  Product_Tax_Code,
  EAN,
  Size,
  SAFE_CAST(Cost AS FLOAT64) AS Cost,
  SAFE_CAST(MRP AS FLOAT64) AS MRP,
  JSON_EXTRACT_SCALAR(Packaging_Dimensions,'$.height') AS Packaging_height,
  JSON_EXTRACT_SCALAR(Packaging_Dimensions,'$.length') AS Packaging_length,
  JSON_EXTRACT_SCALAR(Packaging_Dimensions,'$.width') AS Packaging_width,
  JSON_EXTRACT_SCALAR(Packaging_Dimensions,'$.weight') AS Packaging_weight,
  CASE WHEN Product_Weight_grams_ IS NULL THEN 0.0 ELSE CAST(Product_Weight_grams_ AS FLOAT64) END AS Product_Weight_grams_,
  CASE WHEN Product_Height IS NULL THEN 0.0 ELSE CAST(Product_Height AS FLOAT64) END AS Product_Height,
  CASE WHEN Product_Length IS NULL THEN 0.0 ELSE CAST(Product_Length AS FLOAT64) END AS Product_Length,
  CASE WHEN Product_Width IS NULL THEN 0.0 ELSE CAST(Product_Width AS FLOAT64) END AS Product_Width,
  Payment_Mode,
  Payment_Gateway,
  Payment_Transaction_ID,
  Listing_Ref_No,
  Checkout_ID,
  Discount_Codes,
  Buyer_GST_Num,
  Shipping_Customer_Name,
  Mobile_No,
  Shipping_Address_Line_1,
  Shipping_Address_Line_2,
  Customer_Email,
  Shipping_City,
  Shipping_Zip_Code,
  Shipping_State,
  Shipping_Country,
  Billing_Customer_Name,
  Billing_Address_Line_1,
  Billing_Address_Line_2,
  Billing_City,
  Billing_Zip_Code,
  Billing_State,
  Billing_Country,
  Country_Code,
  Parent_Currency_Multiplier,
  CASE WHEN Order_Invoice_Amount IS NULL THEN 0.0 ELSE CAST(Order_Invoice_Amount AS FLOAT64) END AS Order_Invoice_Amount,
  CASE WHEN Order_Invoice_Amount_Acc_to_Parent_Currency IS NULL THEN 0.0 ELSE CAST(Order_Invoice_Amount_Acc_to_Parent_Currency AS FLOAT64) END AS Order_Invoice_Amount_Acc_to_Parent_Currency,
  CASE WHEN TCS_Rate IS NULL THEN 0.0 ELSE CAST(TCS_Rate AS FLOAT64) END AS TCS_Rate,
  CASE WHEN TCS_Amount IS NULL THEN 0.0 ELSE CAST(TCS_Amount AS FLOAT64) END AS TCS_Amount,
  CASE WHEN Selling_Price IS NULL THEN 0.0 ELSE CAST(Selling_Price AS FLOAT64) END AS Selling_Price,
  CASE WHEN Selling_Price_Acc_to_Parent_Currency IS NULL THEN 0.0 ELSE CAST(Selling_Price_Acc_to_Parent_Currency AS FLOAT64) END AS Selling_Price_Acc_to_Parent_Currency,
  Tax_Type,
  CASE WHEN Tax_Rate IS NULL THEN 0.0 ELSE CAST(Tax_Rate AS FLOAT64) END AS Tax_Rate,
  CASE WHEN Order_Collectable_Amount IS NULL THEN 0.0 ELSE CAST(Order_Collectable_Amount AS FLOAT64) END AS Order_Collectable_Amount,
  CASE WHEN Tax IS NULL THEN 0.0 ELSE CAST(Tax AS FLOAT64) END AS Tax,
  CASE WHEN Item_Price_Excluding_Tax IS NULL THEN 0.0 ELSE CAST(Item_Price_Excluding_Tax AS FLOAT64) END AS Item_Price_Excluding_Tax,
  Custom_Fields,
  Sales_Channel,
  Accounting_Sku,
  Accounting_Unit,
  ERP_Customer_ID,
  EE_Client_ID,
  Shipment_Weight_g_,
  CASE WHEN Expected_Delivery_Date = 'nan' then DATE('0001-01-01') ELSE CAST(CAST(Expected_Delivery_Date AS DATETIME) AS DATE) END AS Expected_Delivery_Date,
  CASE WHEN Hold_Datetime = 'nan' then DATE('0001-01-01') ELSE CAST(CAST(Hold_Datetime AS DATETIME) AS DATE) END AS Hold_Datetime,
  CASE WHEN Unhold_Datetime = 'nan' then DATE('0001-01-01') ELSE CAST(CAST(Unhold_Datetime AS DATETIME) AS DATE) END AS Unhold_Datetime,
  CASE WHEN start_date IS NULL then DATE('0001-01-01') ELSE CAST(CAST(start_date AS DATETIME) AS DATE) END AS start_date,
  CASE WHEN end_date IS NULL then DATE('0001-01-01') ELSE CAST(CAST(end_date AS DATETIME) AS DATE) END AS end_date,
  CASE WHEN created_on IS NULL then DATE('0001-01-01') ELSE CAST(CAST(created_on AS DATETIME) AS DATE) END AS created_on,
  GRN_Batch_Codes,
  report_id,
  report_type,
  inventory_type,
  ee_extracted_at
from
(
select distinct
  *,
  row_number() over(partition by Order_Number, Suborder_No,report_id,Seller_GST_Num,Manifest_ID,GRN_Batch_Codes order by ee_extracted_at DESC) as rn
  FROM `shopify-pubsub-project.easycom.mini_sales_report`
)
where rn = 1
-- where Order_Number = "`20250108_Ankita_01" and Suborder_No = "`20250108_Ankita_01" and report_id = '107543706'
-- where Order_Number = "`MYNJ-HEVN180125-4_OR1" and Suborder_No = "`62111173736549725772941" and report_id = '107543706'


