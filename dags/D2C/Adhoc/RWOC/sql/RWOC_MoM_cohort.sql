
with RWOC_customers as
(SELECT 
  customer_id,
  cust_type,
 FROM `shopify-pubsub-project.adhoc_data.RWOC`
),

 Order_level as(
select
customer_id,
order_name,
-- order_datetime,
datetime(order_datetime,'Asia/Kolkata') as order_datetime,
sum(final_sale_revenue) as Order_value,
FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
where channel = 'Shopify' and customer_id!='0' and order_status not in ('cancelled','refunded')
-- and datetime(order_datetime,'Asia/Kolkata') <='2024-12-13'
group by All
 ),
shopify_table as
(
select 
customer_id,
order_name,
order_datetime,
Order_value,
row_number() over(partition by customer_id order by order_datetime) as ranking,
from Order_level
),


final_base as
(
  select
  RW.customer_id,
  RW.cust_type,
  ST.order_name,
  ST.Order_value,
  ST.order_datetime,
  ST.ranking
  from RWOC_customers as RW
  left join shopify_table as ST
  on RW.customer_id = ST.customer_id

),

-- This cte is used to calculate the very first order date of the customer which is called as first_tarns
aquisition as(
select 
distinct
customer_id,
order_datetime as first_trans_date,
from final_base
where ranking = 1

),

-- Joining the base table and aquisition so that we can get the cohort year month along side of order date for each customer which help to easily compare the diff of order date and cohort date
Retention as (
select
distinct
B.customer_id,
B.cust_type,
B.ranking,
B.order_name,
B.Order_value,
B.order_datetime,
A.first_trans_date,
DATE_DIFF(DATE(order_datetime),DATE(first_trans_date), MONTH) as diffM,
DATE_DIFF(DATE(order_datetime),DATE(first_trans_date), day) as diffD

from final_base as B
left join aquisition as A
using(customer_id)
where B.order_name is not null
),


-- and DATE(order_datetime) >= DATE(DATE_TRUNC(CURRENT_DATE, MONTH)- INTERVAL 1 MONTH - INTERVAL 2 YEAR)
-- and DATE(order_datetime) <= DATE_TRUNC(CURRENT_DATE, MONTH)-1

labelled as (select 
customer_id,
cust_type,
ranking,
order_name,
Order_value,
order_datetime,
first_trans_date,
diffM,
diffD,

    case when diffM = 0 and ranking = 1 then 1 else 0 end as Acq,
    case when diffM = 0 and ranking > 1 then 1 else 0 end as M0,
    case when diffM = 1 and ranking > 1 then 1 else 0 end as M1, 
    case when diffM = 2 and ranking > 1 then 1 else 0 end as M2,
    case when diffM = 3 and ranking > 1 then 1 else 0 end as M3,
    case when diffM = 4 and ranking > 1 then 1 else 0 end as M4,    
    case when diffM = 5 and ranking > 1 then 1 else 0 end as M5,
    case when diffM = 6 and ranking > 1 then 1 else 0 end as M6,
    case when diffM = 7 and ranking > 1 then 1 else 0 end as M7,
    case when diffM = 8 and ranking > 1 then 1 else 0 end as M8,
    case when diffM = 9 and ranking > 1 then 1 else 0 end as M9,
    case when diffM = 10 and ranking > 1 then 1 else 0 end as M10,
    case when diffM = 11 and ranking > 1 then 1 else 0 end as M11,
    case when diffM = 12 and ranking > 1 then 1 else 0 end as M12,
    case when diffM > 12 and ranking > 1 then 1 else 0 end as M12_plus,

from Retention
),


overall as(
select  
DATE_TRUNC(date(first_trans_date), MONTH) as month,
cust_type,
count(distinct case when Acq=1 then customer_id end) as Acq_cust,
count(distinct case when M0=1 then customer_id end)   M0,
count(distinct case when M1=1 then customer_id end)   M1,
count(distinct case when M2=1 then customer_id end)   M2,
count(distinct case when M3=1 then customer_id end)   M3,
count(distinct case when M4=1 then customer_id end)   M4,
count(distinct case when M5=1 then customer_id end)   M5,
count(distinct case when M6=1 then customer_id end)   M6,
count(distinct case when M7=1 then customer_id end)   M7,
count(distinct case when M8=1 then customer_id end)   M8,
count(distinct case when M9=1 then customer_id end)   M9,
count(distinct case when M10=1 then customer_id end)   M10,
count(distinct case when M11=1 then customer_id end)   M11,
count(distinct case when M12=1 then customer_id end)   M12,
count(distinct case when M12_plus = 1 then customer_id end)   M12_plus,


count(distinct case when Acq=1 then order_name end) as Acq_Ord,
count(distinct case when M0=1 then order_name end)   M0_O,
count(distinct case when M1=1 then order_name end)   M1_O,
count(distinct case when M2=1 then order_name end)   M2_O,
count(distinct case when M3=1 then order_name end)   M3_O,
count(distinct case when M4=1 then order_name end)   M4_O,
count(distinct case when M5=1 then order_name end)   M5_O,
count(distinct case when M6=1 then order_name end)   M6_O,
count(distinct case when M7=1 then order_name end)   M7_O,
count(distinct case when M8=1 then order_name end)   M8_O,
count(distinct case when M9=1 then order_name end)   M9_O,
count(distinct case when M10=1 then order_name end)  M10_O,
count(distinct case when M11=1 then order_name end)  M11_O,
count(distinct case when M12=1 then order_name end)  M12_O,
count(distinct case when M12_plus = 1 then order_name end)   M12_Oplus,

sum(case when Acq=1 then Order_value end) as Acq_Sale,
sum(case when M0=1 then Order_value end)   M0_S,
sum(case when M1=1 then Order_value end)   M1_S,
sum(case when M2=1 then Order_value end)   M2_S,
sum(case when M3=1 then Order_value end)   M3_S,
sum(case when M4=1 then Order_value end)   M4_S,
sum(case when M5=1 then Order_value end)   M5_S,
sum(case when M6=1 then Order_value end)   M6_S,
sum(case when M7=1 then Order_value end)   M7_S,
sum(case when M8=1 then Order_value end)   M8_S,
sum(case when M9=1 then Order_value end)   M9_S,
sum(case when M10=1 then Order_value end)  M10_S,
sum(case when M11=1 then Order_value end)  M11_S,
sum(case when M12=1 then Order_value end)  M12_S,
sum(case when M12_plus = 1 then Order_value end)   M12_Splus,


from labelled
group by 1,2
order by 1
)




select * from overall
-- where labelled.first_trans_date<='2024-12-01'


