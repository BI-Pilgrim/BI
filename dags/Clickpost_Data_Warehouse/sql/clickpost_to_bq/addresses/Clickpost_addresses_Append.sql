MERGE INTO `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Addresses` AS target 
USING(
  SELECT
      `Order ID`,
      `Pickup Name`,
      `Pickup Phone`,
      `Pickup Address`,
      `Pickup Pincode`,
      `Pickup City`,
      `Pickup State`,
      `Pickup Country`,
      `Pickup District`,
      `Pickup Organisation`,
      `PickUp Address Type`,
      `Drop Name`,
      `Drop Phone`,
      `Drop Email`,
      `Drop Address`,
      `Drop Pincode`,
      `Drop City`,
      `Drop District`,
      `Drop Organisation`,
      `Drop Address Type`,
      `Return Name`,
      `Return Phone`,
      `Return Address`,
      `Return Pincode`,
      `Return City`,
      `Return State`,
      `Return Country`,
      `Ingestion Date` 
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY `Order ID` ORDER BY `Ingestion Date` DESC) AS row_num
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.addresses` WHERE `Order ID` IS NOT NULL
  )
WHERE row_num = 1 
) AS source  on target.Order_ID = source.`Order ID` 
WHEN MATCHED AND source.`Ingestion Date` > target.Ingestion_Date THEN UPDATE SET 
target.Order_ID							=	source.`Order ID`,
target.Pickup_Name					=	source.`Pickup Name`,
target.Pickup_Phone					=	source.`Pickup Phone`,
target.Pickup_Address				=	source.`Pickup Address`,
target.Pickup_Pincode				=	source.`Pickup Pincode`,
target.Pickup_City					=	source.`Pickup City`,
target.Pickup_State					=	source.`Pickup State`,
target.Pickup_Country				=	source.`Pickup Country`,
target.Pickup_District			=	source.`Pickup District`,
target.Pickup_Organisation	=	source.`Pickup Organisation`,
target.PickUp_Address_Type	=	source.`PickUp Address Type`,
target.Drop_Name						=	source.`Drop Name`,
target.Drop_Phone						=	source.`Drop Phone`,
target.Drop_Email						=	source.`Drop Email`,
target.Drop_Address					=	source.`Drop Address`,
target.Drop_Pincode					=	source.`Drop Pincode`,
target.Drop_City						=	source.`Drop City`,
target.Drop_District				=	source.`Drop District`,
target.Drop_Organisation		=	source.`Drop Organisation`,
target.Drop_Address_Type		=	source.`Drop Address Type`,
target.Return_Name					=	source.`Return Name`,
target.Return_Phone					=	source.`Return Phone`,
target.Return_Address				=	source.`Return Address`,
target.Return_Pincode				=	source.`Return Pincode`,
target.Return_City					=	source.`Return City`,
target.Return_State					=	source.`Return State`,
target.Return_Country				=	source.`Return Country`,
target.Ingestion_Date				=	source.`Ingestion Date` 
WHEN NOT MATCHED THEN INSERT (
Order_ID,
Pickup_Name,
Pickup_Phone,
Pickup_Address,
Pickup_Pincode,
Pickup_City,
Pickup_State,
Pickup_Country,
Pickup_District,
Pickup_Organisation,
PickUp_Address_Type,
Drop_Name,
Drop_Phone,
Drop_Email,
Drop_Address,
Drop_Pincode,
Drop_City,
Drop_District,
Drop_Organisation,
Drop_Address_Type,
Return_Name,
Return_Phone,
Return_Address,
Return_Pincode,
Return_City,
Return_State,
Return_Country,
Ingestion_Date 
) 
VALUES( 
  `Order ID`,
  `Pickup Name`,
  `Pickup Phone`,
  `Pickup Address`,
  `Pickup Pincode`,
  `Pickup City`,
  `Pickup State`,
  `Pickup Country`,
  `Pickup District`,
  `Pickup Organisation`,
  `PickUp Address Type`,
  `Drop Name`,
  `Drop Phone`,
  `Drop Email`,
  `Drop Address`,
  `Drop Pincode`,
  `Drop City`,
  `Drop District`,
  `Drop Organisation`,
  `Drop Address Type`,
  `Return Name`,
  `Return Phone`,
  `Return Address`,
  `Return Pincode`,
  `Return City`,
  `Return State`,
  `Return Country`,
  `Ingestion Date` 
) 

