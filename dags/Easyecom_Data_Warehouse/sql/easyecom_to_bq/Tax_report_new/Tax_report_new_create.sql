CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Tax_report_new`
AS
select
*
from
(
select distinct
-- regex expression to remove trailing decimal and digits after decimal from the strings
REGEXP_REPLACE(Company_Name,'\\.\\d+', '') as Company_Name,
REGEXP_REPLACE(Seller_GST_Num,'\\.\\d+', '') as Seller_GST_Num,
REGEXP_REPLACE(MP_Name,'\\.\\d+', '') as MP_Name,
REGEXP_REPLACE(Reference_Code,'\\.\\d+', '') as Reference_Code,
REGEXP_REPLACE(Suborder_No,'\\.\\d+', '') as Suborder_No,
REGEXP_REPLACE(Order_Type,'\\.\\d+', '') as Order_Type,
REGEXP_REPLACE(EE_Invoice_No,'\\.\\d+', '') as EE_Invoice_No,
REGEXP_REPLACE(MP_Ref_No,'\\.\\d+', '') as MP_Ref_No,
REGEXP_REPLACE(Order_Status,'\\.\\d+', '') as Order_Status,
REGEXP_REPLACE(Invoice_Status,'\\.\\d+', '') as Invoice_Status,
REGEXP_REPLACE(Credit_Note_ID,'\\.\\d+', '') as Credit_Note_ID,
-- date(timestamp_micros(Return_Date/1000)) as Return_Date,
-- DATE(TIMESTAMP_MICROS(Return_Date / 1000)) AS parsed_date,
DATE(TIMESTAMP_MICROS(CAST(Return_Date / 1000 AS INT64))) as Return_Date,
DATE(TIMESTAMP_MICROS(CAST(Import_Date / 1000 AS INT64))) as Import_Date,
DATE(TIMESTAMP_MICROS(CAST(Order_Date / 1000 AS INT64))) as Order_Date,
DATE(TIMESTAMP_MICROS(CAST(Invoice_Date / 1000 AS INT64))) as Invoice_Date,
REGEXP_REPLACE(CR_Voucher_Num,'\\.\\d+', '') as CR_Voucher_Num,
-- Import_Date,
-- Order_Date,
-- Invoice_Date,
REGEXP_REPLACE(Parent_Courier_Partner,'\\.\\d+', '') as Parent_Courier_Partner,
REGEXP_REPLACE(Courier,'\\.\\d+', '') as Courier,
REGEXP_REPLACE(AWB_No,'\\.\\d+', '') as AWB_No,
Parent_Quantity,
Item_Quantity,
REGEXP_REPLACE(Product_Config,'\\.\\d+', '') as Product_Config,
REGEXP_REPLACE(Parent_SKU,'\\.\\d+', '') as Parent_SKU,
Parent_SKU_Weight,
REGEXP_REPLACE(Component_SKU,'\\.\\d+', '') as Component_SKU,
Component_SKU_Weight,
REGEXP_REPLACE(Component_SKU_Description,'\\.\\d+', '') as Component_SKU_Description,
REGEXP_REPLACE(Component_SKU_Name,'\\.\\d+', '') as Component_SKU_Name,
REGEXP_REPLACE(Component_SKU_HSN,'\\.\\d+', '') as Component_SKU_HSN,
REGEXP_REPLACE(Component_SKU_Category,'\\.\\d+', '') as Component_SKU_Category,
REGEXP_REPLACE(Component_SKU_Brand,'\\.\\d+', '') as Component_SKU_Brand,
Component_SKU_Cost,
Component_SKU_MRP,
REGEXP_REPLACE(Payment_Mode,'\\.\\d+', '') as Payment_Mode,
REGEXP_REPLACE(Buyer_GST_Num,'\\.\\d+', '') as Buyer_GST_Num,
REGEXP_REPLACE(Customer_Name,'\\.\\d+', '') as Customer_Name,
REGEXP_REPLACE(Mobile_No,'\\.\\d+', '') as Mobile_No,
REGEXP_REPLACE(Address_Line_1,'\\.\\d+', '') as Address_Line_1,
REGEXP_REPLACE(Address_Line_2,'\\.\\d+', '') as Address_Line_2,
REGEXP_REPLACE(Customer_Email,'\\.\\d+', '') as Customer_Email,
REGEXP_REPLACE(City,'\\.\\d+', '') as City,
REGEXP_REPLACE(Zip_Code,'\\.\\d+', '') as Zip_Code,
REGEXP_REPLACE(State,'\\.\\d+', '') as State,
REGEXP_REPLACE(Tax_Type,'\\.\\d+', '') as Tax_Type,
Tax_Rate,
Sr_No,
Collectible_Amount,
Order_Invoice_Amount,
TCS_Rate,
TCS,
Selling_Price,
Wallet_Discount,
Item_Price_Excluding_Tax,
COD_Excluding_Tax,
Shipping_Charge_Excluding_Tax,
Shipping_Discount_Excluding_Tax,
Promotion_Discount_Excluding_Tax,
Miscellaneous_Excluding_Tax,
Gift_Wrap_Charges_Excluding_Tax,
Prepaid_Discount_Excluding_Tax,
Promocode_Discount_Excluding_Tax,
Taxable_Value,
Tax,
IGST,
CGST,
SGST,
CESS,
UTGST,
TDS,
IRN_Number,
Acknowledgement_Num,
DATE(TIMESTAMP_MICROS(CAST(Acknowledgement_Date / 1000 AS INT64))) as Acknowledgement_Date,
DATE(TIMESTAMP_MICROS(CAST(Eway_Bill_Date / 1000 AS INT64))) as Eway_Bill_Date,
-- Acknowledgement_Date,
Eway_Bill_Number,
-- Eway_Bill_Date,
CRN_Number,
CreditNote_Acknowledgement_Num,
DATE(TIMESTAMP_MICROS(CAST(CreditNote_Acknowledgement_Date / 1000 AS INT64))) as CreditNote_Acknowledgement_Date,
-- CreditNote_Acknowledgement_Date,
REGEXP_REPLACE(Debit_Note_Number,'\\.\\d+', '') as Debit_Note_Number,
REGEXP_REPLACE(Sales_Channel,'\\.\\d+', '') as Sales_Channel,
-- Manifest_Date,
DATE(TIMESTAMP_MICROS(CAST(Manifest_Date / 1000 AS INT64))) as Manifest_Date,
REGEXP_REPLACE(ERP_Customer_ID,'\\.\\d+', '') as ERP_Customer_ID,
REGEXP_REPLACE(EE_Client_ID,'\\.\\d+', '') as EE_Client_ID,
REGEXP_REPLACE(Invoice_ERP_Transaction_ID,'\\.\\d+', '') as Invoice_ERP_Transaction_ID,
REGEXP_REPLACE(Billing_Customer_Name,'\\.\\d+', '') as Billing_Customer_Name,
REGEXP_REPLACE(Billing_Mobile_No,'\\.\\d+', '') as Billing_Mobile_No,
REGEXP_REPLACE(Billing_Customer_Email,'\\.\\d+', '') as Billing_Customer_Email,
REGEXP_REPLACE(Billing_Address_Line_1,'\\.\\d+', '') as Billing_Address_Line_1,
REGEXP_REPLACE(Billing_Address_Line_2,'\\.\\d+', '') as Billing_Address_Line_2,
REGEXP_REPLACE(Billing_City,'\\.\\d+', '') as Billing_City,
REGEXP_REPLACE(Billing_Zip_Code,'\\.\\d+', '') as Billing_Zip_Code,
REGEXP_REPLACE(Billing_State,'\\.\\d+', '') as Billing_State,
REGEXP_REPLACE(Billing_Country,'\\.\\d+', '') as Billing_Country,
REGEXP_REPLACE(A_c_SKu,'\\.\\d+', '') as A_c_SKu,
A_c_Unit,
REGEXP_REPLACE(External_Order_Code,'\\.\\d+', '') as External_Order_Code,
GRN_Cost_per_unit,
Additional_cost_per_unit,
Cost_of_Goods_Purchased,
DATE(TIMESTAMP_MICROS(CAST(Delivery_Appointment_Date / 1000 AS INT64))) as Delivery_Appointment_Date,
-- Delivery_Appointment_Date,
REGEXP_REPLACE(report_id,'\\.\\d+', '') as report_id,
REGEXP_REPLACE(report_type,'\\.\\d+', '') as report_type,
start_date,
end_date,
created_on,
inventory_type,
ee_extracted_at,
row_number() over(partition by Reference_Code, Suborder_No, Component_SKU,report_id order by ee_extracted_at desc) as rn
from shopify-pubsub-project.easycom.tax_reports 
)
where rn = 1
