-- #5  State, month wise AOV across Male Female,Prepaid COD, New Repeat

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
  on CO.customer_id = A.customer_id),

metric_Calc as
(  select 
    date(date_trunc(Order_created_at,month)) as Month,
    -- Order_fulfillment_status,
    -- coalesce(Gender,'No_mapping') as Gender,
    -- Customer_State,
    Customer_pincode,
    -- Payment_type,
    -- New_Repeat_Tag,
    
    count(distinct case when Payment_type = 'Prepaid' then customer_id end) as prepaid_customer_count,
    count(distinct case when Payment_type = 'COD' then customer_id end) as COD_customer_count,
    count(distinct case when Gender = 'male' then customer_id end) as Male_customer_count,
    count(distinct case when Gender = 'female' then customer_id end) as Female_customer_count,
    count(distinct case when New_Repeat_Tag = 'New' then customer_id end) as New_customer_count,
    count(distinct case when New_Repeat_Tag = 'Repeat' then customer_id end) as Repeat_customer_count,
    count(distinct customer_id) as Total_customer,

    count(distinct case when Payment_type = 'Prepaid' then Order_name end) as prepaid_order_count,
    count(distinct case when Payment_type = 'COD' then Order_name end) as COD_order_count,
    count(distinct case when Gender = 'male' then Order_name end) as Male_order_count,
    count(distinct case when Gender = 'female' then Order_name end) as Female_order_count,
    count(distinct case when New_Repeat_Tag = 'New' then Order_name end) as New_order_count,
    count(distinct case when New_Repeat_Tag = 'Repeat' then Order_name end) as Repeat_order_count,
    count(distinct Order_name) as Total_Orders,

    sum(case when Payment_type = 'Prepaid' then Order_total_price end) as prepaid_revenue,
    sum(case when Payment_type = 'COD' then Order_total_price end) as COD_revenue,
    sum(case when Gender = 'male' then Order_total_price end) as Male_revenue,
    sum(case when Gender = 'female' then Order_total_price end) as Female_revenue,
    sum(case when New_Repeat_Tag = 'New' then Order_total_price end) as New_revenue,
    sum(case when New_Repeat_Tag = 'Repeat' then Order_total_price end) as Repeat_revenue,
    sum(Order_total_price) as Total_Revenue,



  from Base
  where 1=1 
  and Order_fulfillment_status = 'fulfilled'
  and Payment_type not in ('Cancelled')
  and date_trunc(Order_created_at,month) > '2024-09-01'
  group by ALL
)

select 

B.City,
B.Tier,
B.State,
A.Month,
A.prepaid_customer_count,
A.COD_customer_count,
A.Male_customer_count,
A.Female_customer_count,
A.New_customer_count,
A.Repeat_customer_count,
A.Total_customer,
A.prepaid_order_count,
A.COD_order_count,
A.Male_order_count,
A.Female_order_count,
A.New_order_count,
A.Repeat_order_count,
A.Total_Orders,
A.prepaid_revenue,
A.COD_revenue,
A.Male_revenue,
A.Female_revenue,
A.New_revenue,
A.Repeat_revenue,
A.Total_Revenue,
coalesce(B.Zone,'OOI') as Region,
 from metric_Calc as A
 left join `shopify-pubsub-project.Project_Goonj_asia.Pincode_Tier_City_State_mapping` as B
 on A.Customer_pincode = B.Pincodes
 where month = '2025-02-01';
