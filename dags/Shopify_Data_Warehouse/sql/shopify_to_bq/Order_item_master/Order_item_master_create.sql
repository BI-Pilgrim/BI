
CREATE or replace TABLE `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items_master`
PARTITION BY DATE_TRUNC(Order_created_at,day)
 
OPTIONS(
 description = "Orderitem Master table is partitioned on Order created at day level",
 require_partition_filter = False
 )
 AS
with Orders_cte as 
(
    SELECT 
    customer_id,
    order_name,
    discount_final,
    max(Datetime(Order_created_at, "Asia/Kolkata")) as Order_created_at,
    sum(total_tax) as total_tax,
    sum(Order_total_price) as Order_total_price,
    
  FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` 
  WHERE 1=1 

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
    when PM.Parent_SKU like 'SAM%' or PM.Parent_SKU like '%MINI%' or PM.Parent_SKU like '%GIFT%' or PM.Parent_SKU like '%PG-CL25%'
    or PM.Parent_SKU like '%PG-GMP1%' then 0 
    else PM.MRP_OG end as item_MRP_price,

    (case when PM.Parent_SKU like 'PGJB%' then 28 
    when PM.Parent_SKU like 'SAM%' or PM.Parent_SKU like '%MINI%' or PM.Parent_SKU like '%GIFT%'  or PM.Parent_SKU like '%PG-CL25%'
    or PM.Parent_SKU like '%PG-GMP1%'  then 0 
    else PM.MRP_OG end)*item_quantity as item_GMV,

    
    
    from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items` as OI
    left join `shopify-pubsub-project.Product_SKU_Mapping.D2C_SKU_mapping` as PM
    on cast(OI.item_variant_id as string)= PM.variant_id
    where 1=1
  ),

derived_GMV as
(  select 
distinct
  O.customer_id,
  O.order_name,
  O.Order_created_at,
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
    DG.Order_created_at,
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
