
CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_Amazon_Seller_Staging.Order_items`
PARTITION BY DATE_TRUNC(LastUpdateDate, DAY)
CLUSTER BY Amazon_Order_id
OPTIONS(
  description = "Orders Items table is partitioned on last update date",
  require_partition_filter = FALSE
)
 AS 
SELECT  
distinct
_airbyte_extracted_at,
ASIN,
Title,
json_extract_scalar(CODFee,"$.Amount") as COD_fee,
IsGift,
json_extract_scalar(ItemTax,"$.Amount") as Item_Tax,
json_extract_scalar(ItemPrice,"$.Amount") as Item_price,
SellerSKU,
ConditionId,
OrderItemId,
json_extract_scalar(ProductInfo,"$.NumberOfItems") as Number_of_items,
json_extract_scalar(ShippingTax,"$.Amount") as Shipping_tax,
json_extract_string_array(PromotionIds) as Promotion_ids,
AmazonOrderId as Amazon_Order_id,
json_extract_scalar(ShippingPrice,"$.Amount") as Shipping_price,
IsTransparency,
LastUpdateDate,
QuantityOrdered,
QuantityShipped,
PriceDesignation,
json_extract_scalar(ShippingDiscount,"$.Amount") as Shipping_discount,
json_extract_scalar(PromotionDiscount,"$.Amount") as promotion_discount,
ConditionSubtypeId,
json_extract_scalar(BuyerRequestedCancel,"$.BuyerCancelReason") as Cancel_reason,
json_extract_scalar(BuyerRequestedCancel,"$.IsBuyerRequestedCancel") as Buyer_requested_cancel,
SerialNumberRequired
FROM `shopify-pubsub-project.pilgrim_bi_airbyte_amazon_seller.OrderItems`
