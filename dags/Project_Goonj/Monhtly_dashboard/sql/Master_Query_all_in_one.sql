-- Project GOONJ ALL Table Query in one 

-- #1 This will read  the data from Google Play store and Create a aggregated table 
create or Replace Table `shopify-pubsub-project.Project_Goonj_asia.Google_Playstore_Rating`
as
with raw_rating as 
( 
    select 
    date(date_trunc(review_given_at,month)) as Year_month,
    avg(score) as Avg_rating,
    sum(score) as Scores,
    count(review_id) as Customers
    from  `shopify-pubsub-project.pilgrim_bi_google_play.google_play_ratings`
    group by all
    order by 1),
    
base as 
(
    select 
    *,
    SUM(Scores) OVER (ORDER BY year_month) as Score_freq,
    SUM(Customers) OVER (ORDER BY year_month) as Customer_freq
    from raw_rating
    order by 1
    )

select 
*,
Score_freq/Customer_freq as Cumulative_Rating
from base;


-- #2 This will combine the ratings from Flipkart and Amazon Rating into one 
create or Replace Table `shopify-pubsub-project.Project_Goonj_asia.Marketplace_Combined_Ratings`
as

with combine as 
(SELECT 
distinct 
'Flipkart' as Channel,
SKU_ID,
Title,
date_trunc(Scraped_date,month) as Scraped_date,
avg(Avg_rating) as Avg_rating
 FROM `shopify-pubsub-project.Project_Goonj_asia.Flipkart_Ratings_Top_Products`
group by ALL

union all

select
distinct
'Amazon' as Channel,
SKU,
Product_Title,
date(date_trunc(Scraped_date,month)) as Scraped_date,
avg(Avg_rating) as Avg_rating
from `shopify-pubsub-project.Project_Goonj_asia.AMZ_Ratings_Top_products`
group by ALL
)
select 
  C.Channel ,
  SKU_ID,
  coalesce(Product_Title,Title,SKU_ID,'') as Product_Title,
  Scraped_date,
  Avg_rating
 
 from combine as C
 left join 
 (select
distinct
Parent_SKU,
Product_Title,
from `shopify-pubsub-project.adhoc_data_asia.Product_SKU_mapping_D2C`) as M
on C.SKU_ID = M.Parent_SKU;



-- #3 This will create a customer metrics across new repeat, prepaid COD and Male female month wise

create or Replace Table `shopify-pubsub-project.Project_Goonj_asia.Customer_metrics_NR_PC_MF`
as
with cust as 
(select 
distinct
DATETIME(CT.customer_created_at, "Asia/Kolkata") as customer_created_at,
CT.customer_id,
CT.Customer_province,
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
  -- billing_zip,
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
    Payment_type,
    New_Repeat_Tag,
    count(distinct customer_id) as customer_count,
    count(distinct Order_name) as Order_count,
    sum(Order_total_price) as Order_total_price,
    sum(Order_quantity) as Order_quantity

  from Base
  where 1=1 
  and Order_fulfillment_status = 'fulfilled'
  and Payment_type not in ('Cancelled')
  and date_trunc(Order_created_at,month) > '2024-09-01'
  group by all
  order by 1 desc;


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


-- #5  State, month wise AOV across Male Female,Prepaid COD, New Repeat

create or Replace Table `shopify-pubsub-project.Project_Goonj_asia.State_wise_AOV`
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
  on CO.customer_id = A.customer_id),

metric_Calc as
(  select 
    date(date_trunc(Order_created_at,month)) as Month,
    -- Order_fulfillment_status,
    -- coalesce(Gender,'No_mapping') as Gender,
    Customer_State,
    -- Customer_pincode,
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
*,
prepaid_revenue/prepaid_order_count as prepaid_AOV,
COD_revenue/COD_order_count as COD_AOV,
Male_revenue/Male_order_count as Male_AOV,
Female_revenue/Female_order_count as Female_AOV,
New_revenue/New_order_count as New_AOV,
Repeat_revenue/Repeat_order_count as Repeat_AOV,
Total_Revenue/Total_Orders as Overall_AOV,
 from metric_Calc;
