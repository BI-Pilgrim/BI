

with Order_level as(
select
customer_email,
customer_id,
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
customer_email,
customer_id,
order_name,
order_datetime,
Order_value,
row_number() over(partition by customer_id order by order_datetime) as ranking,
from Order_level
),



-- This cte is used to calculate the very first order date of the customer which is called as first_tarns
acquisition as(
select 
distinct
customer_email,
customer_id,
date(order_datetime) as first_trans_date,
from shopify_table
where ranking = 1

)

select 
distinct
first_trans_date,
customer_email,
customer_id,
from acquisition
where first_trans_date>'2024-12-12'

