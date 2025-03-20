CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Addresses` 
PARTITION BY Ingestion_Date
AS 
SELECT 
    `Order ID`AS Order_ID,
    `Pickup Name`AS Pickup_Name,
    `Pickup Phone`AS Pickup_Phone,
    `Pickup Address`AS Pickup_Address,
    `Pickup Pincode`AS Pickup_Pincode,
    `Pickup City`AS Pickup_City,
    `Pickup State`AS Pickup_State,
    `Pickup Country`AS Pickup_Country,
    `Pickup District`AS Pickup_District,
    `Pickup Organisation`AS Pickup_Organisation,
    `PickUp Address Type`AS PickUp_Address_Type,
    `Drop Name`AS Drop_Name,
    `Drop Phone`AS Drop_Phone,
    `Drop Email`AS Drop_Email,
    `Drop Address`AS Drop_Address,
    `Drop Pincode`AS Drop_Pincode,
    `Drop City`AS Drop_City,
    `Drop District`AS Drop_District,
    `Drop Organisation`AS Drop_Organisation,
    `Drop Address Type`AS Drop_Address_Type,
    `Return Name`AS Return_Name,
    `Return Phone`AS Return_Phone,
    `Return Address`AS Return_Address,
    `Return Pincode`AS Return_Pincode,
    `Return City`AS Return_City,
    `Return State`AS Return_State,
    `Return Country`AS Return_Country,
    `Ingestion Date`AS Ingestion_Date
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.addresses` WHERE `Order ID` IS NOT NULL
  )
WHERE row_num = 1;
