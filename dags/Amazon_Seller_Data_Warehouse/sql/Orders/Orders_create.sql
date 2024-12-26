
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.Orders`
PARTITION BY DATE_TRUNC(Purchase_date, DAY)
CLUSTER BY Amazon_Order_id
OPTIONS(
  description = "Orders table is partitioned on purchased date",
  require_partition_filter = FALSE
)
 AS 
SELECT  
distinct
_airbyte_extracted_at,
IsISPU,
IsPrime,
JSON_EXTRACT_SCALAR(BuyerInfo, '$.BuyerEmail') AS Buyer_Email,
OrderType,
seller_id,
IsSoldByAB,
JSON_EXTRACT_SCALAR(OrderTotal, '$.Amount') as Order_Amount,
OrderStatus as Order_status,
PurchaseDate as Purchase_date,
SalesChannel,
AmazonOrderId as Amazon_Order_id,
MarketplaceId as Marketplace_id,
PaymentMethod as Payment_type,
SellerOrderId,
IsPremiumOrder,
LastUpdateDate as Order_last_updated_date,
LatestShipDate,
IsBusinessOrder,
JSON_EXTRACT_SCALAR(ShippingAddress, '$.City') as Shipping_City,
JSON_EXTRACT_SCALAR(ShippingAddress,'$.StateOrRegion') as Shipping_State,
JSON_EXTRACT_SCALAR(ShippingAddress,'$.PostalCode') as Shipping_Pincode,

EarliestShipDate,
ShipServiceLevel,
HasRegulatedItems,
FulfillmentChannel,
IsAccessPointOrder,
IsReplacementOrder,
LatestDeliveryDate,
EarliestDeliveryDate,
NumberOfItemsShipped,
JSON_EXTRACT_STRING_ARRAY(PaymentMethodDetails) as Payment_method,
IsGlobalExpressEnabled,
NumberOfItemsUnshipped,
json_extract_scalar(AutomatedShippingSettings,'$.HasAutomatedShippingSettings') as Has_automated_shipping,
ShipmentServiceLevelCategory,
Concat(
JSON_EXTRACT_SCALAR(DefaultShipFromLocationAddress, '$.AddressLine1'), ", ",
JSON_EXTRACT_SCALAR(DefaultShipFromLocationAddress,'$.AddressLine2') 
)as Ship_From_Address,
JSON_EXTRACT_SCALAR(DefaultShipFromLocationAddress,'$.City') as Ship_From_City,
JSON_EXTRACT_SCALAR(DefaultShipFromLocationAddress,'$.Name') as Ship_From_Name,
JSON_EXTRACT_SCALAR(DefaultShipFromLocationAddress,'$.PostalCode') as Ship_From_Pincode,
JSON_EXTRACT_SCALAR(DefaultShipFromLocationAddress,'$.StateOrRegion') as Ship_From_State

FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.Orders`
