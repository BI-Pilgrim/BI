with Orders_cte as 
(
    SELECT 
    customer_id,
    order_name,
    Datetime(Order_created_at, "Asia/Kolkata") as Order_created_at,
    discount_code,
    total_tax,
    Order_total_price,
    
  FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` 
  WHERE 1=1 
  and Order_fulfillment_status = 'fulfilled'
  and Order_financial_status not in ('voided','refunded')

  ),

  Order_item as (
    select
    OI.Order_name,
    OI.order_item_id,
    OI.item_variant_id,
    OI.item_sku_code,
    PM.Product_Title,
    PM.Parent_SKU,
    PM.Main_Category,
    PM.Sub_Category,
    OI.item_quantity,
    case when PM.Parent_SKU like 'PGJB%' then 28 
    when PM.Parent_SKU like 'SAM%' or PM.Parent_SKU like '%MINI%' then 0 
    else PM.MRP_OG end as item_MRP_price,

    (case when PM.Parent_SKU like 'PGJB%' then 28 
    when PM.Parent_SKU like 'SAM%' or PM.Parent_SKU like '%MINI%' then 0 
    else PM.MRP_OG end)*item_quantity as item_GMV,

    
    
    from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items` as OI
    left join `shopify-pubsub-project.adhoc_data_asia.Product_SKU_mapping_D2C` as PM
    on cast(OI.item_variant_id as string)= PM.variant_id
    where 1=1
    and item_fulfillment_status = 'fulfilled'

    

  ),

derived_GMV as
(  select 
  O.customer_id,
  O.order_name,
  O.Order_created_at,
  O.discount_code,
  sum(item_MRP_price*item_quantity) as Order_GMV,
  avg(Order_total_price) as Order_value,
  from Orders_cte as O
  left join Order_item as OI
  using(Order_name)
   
  group by ALL
  order by 1
), 

item_level_base as 
  (    select 
    DG.customer_id,
    OI.Order_name,
    OI.order_item_id,
    DG.Order_created_at,
    DG.discount_code,
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
    where 1=1
  ),

 cust as 
(select 
  distinct
  DATETIME(CT.customer_created_at, "Asia/Kolkata") as customer_created_at,
  CT.customer_id,
  CT.Customer_province,
  CT.Customer_zip,
  case when CM.Gender_field = 'NIL' then CM.Gender_der_field else CM.Gender_field end as Gender,
  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customers` as CT
  left join  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Metafield_customers` as CM
  on CT.customer_id = CM.customer_id

),

 ord as 
(
  select 
  distinct
  customer_id,
  Order_name,
  DATETIME(Order_created_at, "Asia/Kolkata") as Order_created_at,
  billing_zip,
  billing_province,
  Order_fulfillment_status,
  -- discount_code,
  Case when Order_financial_status = 'pending' then 'COD'
    when Order_financial_status in ('partially_paid','paid','partially_refunded') then 'Prepaid'
    when Order_financial_status in ('voided','refunded') then 'Cancelled'
    else 'Others' end as Payment_type,
  sum(total_line_items_price) as total_line_items_price,
  sum(Order_total_price) as Order_total_price,
  sum(Order_total_discounts) as Order_total_discounts,

  from `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders`
  
 group by all
),

order_items as
(
  select 
    Order_name,
    sum(item_quantity) as Order_quantity,
     from  `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items`
  group by all
),

custOrd as 
(
  select 
  O.customer_id,
  O.Order_name,
  O.Order_created_at,
  coalesce(C.Customer_province,O.billing_province) as Customer_State,
  coalesce(C.Customer_zip,O.billing_zip) as Customer_pincode,

  O.Order_fulfillment_status,
  O.Payment_type,
  O.total_line_items_price,
  O.Order_total_price,
  C.Gender,
  OI.Order_quantity,
  row_number() over(partition by O.customer_id order by O.Order_created_at) as ranking 
  from ord as O
  left join cust as C
  on O.customer_id = C.customer_id
  left join order_items as OI
  on OI.order_name = O.order_name
),

acquisition as 
  (
    select 
    customer_id,
    Order_created_at as first_trans_date
    from custOrd
    where ranking = 1
  ),

Base as 
  (select 
  CO.*,
  A.first_trans_date,
  case when Order_created_at = first_trans_date then 'New' else 'Repeat' end as New_Repeat_Tag,
  from custOrd as CO
  left join acquisition as A
  on CO.customer_id = A.customer_id)



select 
*
 from base as B
left join  item_level_base as I
on B.order_name = I.order_name
where B.Order_created_at >'2025-01-01'



