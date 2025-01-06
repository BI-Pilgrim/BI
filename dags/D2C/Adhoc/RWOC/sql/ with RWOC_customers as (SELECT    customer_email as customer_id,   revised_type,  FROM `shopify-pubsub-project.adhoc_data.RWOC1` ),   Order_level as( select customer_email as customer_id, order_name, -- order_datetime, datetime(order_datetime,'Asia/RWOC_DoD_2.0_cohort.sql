
with RWOC_customers as
(SELECT 
  customer_email as customer_id,
  revised_type,
 FROM `shopify-pubsub-project.adhoc_data.RWOC1`
),

 Order_level as(
select
customer_email as customer_id,
order_name,
-- order_datetime,
datetime(order_datetime,'Asia/Kolkata') as order_datetime,
sum(final_sale_revenue) as Order_value,
FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
where channel = 'Shopify' and customer_id!='0' and order_status not in ('cancelled','refunded')
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
  RW.revised_type,
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
B.revised_type,
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
revised_type,
ranking,
order_name,
Order_value,
order_datetime,
first_trans_date,
diffM,
diffD,

    case when diffD = 0 and ranking = 1 then 1 else 0 end as Acq,
    case when diffD = 0 and ranking > 1 then 1 else 0 end as D0,
    case when diffD = 1 and ranking > 1 then 1 else 0 end as D1, 
    case when diffD = 2 and ranking > 1 then 1 else 0 end as D2,
    case when diffD = 3 and ranking > 1 then 1 else 0 end as D3,
    case when diffD = 4 and ranking > 1 then 1 else 0 end as D4,    
    case when diffD = 5 and ranking > 1 then 1 else 0 end as D5,
    case when diffD = 6 and ranking > 1 then 1 else 0 end as D6,
    case when diffD = 7 and ranking > 1 then 1 else 0 end as D7,
    case when diffD = 8 and ranking > 1 then 1 else 0 end as D8,
    case when diffD = 9 and ranking > 1 then 1 else 0 end as D9,
    case when diffD = 10 and ranking > 1 then 1 else 0 end as D10,
    case when diffD = 11 and ranking > 1 then 1 else 0 end as D11,
    case when diffD = 12 and ranking > 1 then 1 else 0 end as D12,
    case when diffD > 12 and ranking > 1 then 1 else 0 end as D12_plus,

from Retention
),


overall as(
select  
DATE_TRUNC(date(first_trans_date), MONTH) as month,
revised_type,
count(distinct case when Acq=1 then customer_id end) as Acq_cust,
count(distinct case when D0=1  then customer_id end)   D0,
count(distinct case when D1=1  then customer_id end)   D1,
count(distinct case when D2=1  then customer_id end)   D2,
count(distinct case when D3=1  then customer_id end)   D3,
count(distinct case when D4=1  then customer_id end)   D4,
count(distinct case when D5=1  then customer_id end)   D5,
count(distinct case when D6=1  then customer_id end)   D6,
count(distinct case when D7=1  then customer_id end)   D7,
count(distinct case when D8=1  then customer_id end)   D8,
count(distinct case when D9=1  then customer_id end)   D9,
count(distinct case when D10=1 then customer_id end) D10,
count(distinct case when D11=1 then customer_id end) D11,
count(distinct case when D12=1 then customer_id end) D12,
count(distinct case when D12_plus=1  then customer_id end)   D12_plus,

count(distinct case when Acq=1 then order_name end) as Acq_Ord,
count(distinct case when D0=1  then order_name end)   D0_O,
count(distinct case when D1=1  then order_name end)   D1_O,
count(distinct case when D2=1  then order_name end)   D2_O,
count(distinct case when D3=1  then order_name end)   D3_O,
count(distinct case when D4=1  then order_name end)   D4_O,
count(distinct case when D5=1  then order_name end)   D5_O,
count(distinct case when D6=1  then order_name end)   D6_O,
count(distinct case when D7=1  then order_name end)   D7_O,
count(distinct case when D8=1  then order_name end)   D8_O,
count(distinct case when D9=1  then order_name end)   D9_O,
count(distinct case when D10=1 then order_name end)  D10_O,
count(distinct case when D11=1 then order_name end)  D11_O,
count(distinct case when D12=1 then order_name end)  D12_O,
count(distinct case when D12_plus=1  then order_name end)  D12_Oplus,

sum(case when Acq=1 then order_value end) as Acq_Sale,
sum(case when D0=1  then order_value end)   D0_S,
sum(case when D1=1  then order_value end)   D1_S,
sum(case when D2=1  then order_value end)   D2_S,
sum(case when D3=1  then order_value end)   D3_S,
sum(case when D4=1  then order_value end)   D4_S,
sum(case when D5=1  then order_value end)   D5_S,
sum(case when D6=1  then order_value end)   D6_S,
sum(case when D7=1  then order_value end)   D7_S,
sum(case when D8=1  then order_value end)   D8_S,
sum(case when D9=1  then order_value end)   D9_S,
sum(case when D10=1 then order_value end) D10_S,
sum(case when D11=1 then order_value end) D11_S,
sum(case when D12=1 then order_value end) D12_S,
sum(case when D12_plus=1  then order_value end)   D12_Splus,



from labelled
group by 1,2
order by 1
)




select * from overall
where month>='2024-12-01'
