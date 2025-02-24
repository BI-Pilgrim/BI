MERGE INTO `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items_master` AS TARGET
USING (

with Orders_cte as 
(
    SELECT 
    customer_id,
    order_name,
    coalesce(discount_application,'')||coalesce(discount_code,'') as discount_final,,
    max(Datetime(Order_processed_at, "Asia/Kolkata")) as Order_processed_at,
    sum(total_tax) as total_tax,
    sum(Order_total_price) as Order_total_price,
    
  FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` 
  -- WHERE 1=1 
  -- and Order_fulfillment_status = 'fulfilled'
  -- and Order_financial_status not in ('voided','refunded')
  group by all
 
  ),

  Order_item as (
    select
    distinct
    OI.Order_name,
    OI.order_item_id,
    OI.item_variant_id,
    OI.item_sku_code,
    OI.item_price,
    PM.Product_Title,
    PM.Parent_SKU,
    PM.Variant_SKU,
    PM.Main_Category,
    PM.Sub_Category,
    PM.Range,
    OI.item_quantity,
    case when PM.Parent_SKU like 'PGJB%' then 28 
    when PM.Parent_SKU like 'SAM%' or PM.Parent_SKU like '%MINI%' then 0 
    else OI.item_price end as item_MRP_price,

    (case when PM.Parent_SKU like 'PGJB%' then 28 
    when PM.Parent_SKU like 'SAM%' or PM.Parent_SKU like '%MINI%' then 0 
    else OI.item_price end)*item_quantity as item_GMV,

    
    
    from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items` as OI
    left join `shopify-pubsub-project.adhoc_data_asia.Product_SKU_mapping_D2C` as PM
    on cast(OI.item_variant_id as string)= PM.variant_id
    -- where 1=1
  ),

derived_GMV as
(  select 
distinct
  O.customer_id,
  O.order_name,
  O.Order_processed_at,
  O.discount_final,
  sum(item_MRP_price*item_quantity) as Order_GMV,
  avg(Order_total_price) as Order_value,
  from Orders_cte as O
  left join Order_item as OI
  using(Order_name)
   
  group by ALL
  order by 1
)

   select 
   distinct
    DG.customer_id,
    OI.Order_name,
    OI.order_item_id,
    DG.Order_processed_at,
    DG.discount_final,
    OI.item_variant_id,
    OI.item_sku_code,
    OI.Parent_SKU,
    OI.Product_Title,
    OI.item_quantity,
    OI.item_MRP_price,
    OI.item_GMV,
    case when DG.Order_GMV = 0 then 1 else  (1-(DG.Order_value/DG.Order_GMV)) end as discount_percentage,
    case when DG.Order_GMV = 0 then 0 else  (DG.Order_value/DG.Order_GMV)*OI.item_GMV end as item_gross_revenue,
    from Order_item as OI
    left join derived_GMV as DG
    using(order_name)
    where date(Order_processed_at) >= DATE_SUB(CURRENT_DATE("Asia/Kolkata"), INTERVAL 30 DAY)
) as Source

on Target.order_item_id = source.order_item_id

when MATCHED THEN UPDATE SET
target.customer_id = source.customer_id,
target.Order_name = source.Order_name,
target.order_item_id = source.order_item_id,
target.Order_processed_at = source.Order_processed_at,
target.discount_final = source.discount_final,
target.item_variant_id = source.item_variant_id,
target.item_sku_code = source.item_sku_code,
target.Parent_SKU = source.Parent_SKU,
target.Product_Title = source.Product_Title,
target.item_quantity = source.item_quantity,
target.item_MRP_price = source.item_MRP_price,
target.item_GMV = source.item_GMV,
target.discount_percentage = source.discount_percentage,
target.item_gross_revenue = source.item_gross_revenue


WHEN NOT MATCHED THEN INSERT 
(
customer_id,
Order_name,
order_item_id,
Order_processed_at,
discount_final,
item_variant_id,
item_sku_code,
Parent_SKU,
Product_Title,
item_quantity,
item_MRP_price,
item_GMV,
discount_percentage,
item_gross_revenue
)
VALUES
(
source.customer_id,
source.Order_name,
source.order_item_id,
source.Order_processed_at,
source.discount_final,
source.item_variant_id,
source.item_sku_code,
source.Parent_SKU,
source.Product_Title,
source.item_quantity,
source.item_MRP_price,
source.item_GMV,
source.discount_percentage,
source.item_gross_revenue
)
