-- #4 state month wise customer metrics 

create or replace Table `shopify-pubsub-project.Project_Goonj_asia.State_level_Metrics`
as
with cust as 
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

order_item as
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
  left join order_item as OI
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
    date(date_trunc(Order_created_at,month)) as Month,
    coalesce(Gender,'No_mapping') as Gender,
    Customer_State,
    Payment_type,
    New_Repeat_Tag,
    count(distinct customer_id) as Customer_count,
    count(distinct order_name) as Order_count,
    sum(Order_total_price) as Order_Price
  from Base
  where 1=1 
  and Order_fulfillment_status = 'fulfilled'
  and Payment_type not in ('Cancelled')
  and date_trunc(Order_created_at,month) > '2024-09-01'
  group by ALL;

