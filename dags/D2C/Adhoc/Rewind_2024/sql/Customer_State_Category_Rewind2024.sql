WITH Customer_State_Sales AS
(
    select 
  customer_email as customer_id,
  case when ship_state in ('Uttarakhand','UTTRAKHAND') then 'Uttarakhand'
  when ship_state in ('Tamilnadu','Tamil Nadu') then 'Tamil Nadu'
  when ship_state in ('Bihar state','Bihar') then 'Bihar'
  else ship_state
  end as state,
   
    CASE WHEN custom_main_category = 'Hair Care' THEN 'Hair Care'
      WHEN custom_main_category IN ('Lip Care', 'Face Care', 'Body Care') THEN 'Skin Care'
      WHEN custom_main_category = 'Makeup' THEN 'Makeup'
      ELSE 'Others'
    END AS custom_main_category,
  sum(final_sale_quantity) as total_qty
  FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
  where customer_id not in ('','0') and order_status not in ('cancelled','refunded') and channel = 'Shopify'
  and order_date>='2024-01-01'
  group by All
),

state_grouping as
(  select 
  customer_id,
  state,
  sum(total_qty) as max_qty
  from Customer_State_Sales
  group by all

),

one_state_mapping as
(
  select 
  *,
  row_number() over(partition by customer_id order by max_qty desc) as ranking
  from state_grouping
),

customer_state_mapping as (
  select 
  distinct
  customer_id,
  state 
  from one_state_mapping
  where ranking = 1
),


base as (
  select 
    A.customer_id,
    B.state,
    custom_main_category,
    total_qty
  from Customer_State_Sales as A
  left join customer_state_mapping as B
  using(customer_id)


),


StateRevenuePercentiles AS (
    SELECT
        distinct
        customer_id,
        state,
        custom_main_category,
        total_qty,
        NTILE(10) OVER (PARTITION BY state,custom_main_category ORDER BY total_qty DESC) AS top_10_percentile
        FROM
        base
),

final_tagging as 
(SELECT
    distinct
    customer_id,
    state,
    CASE WHEN top_10_percentile <= 1  AND custom_main_category = 'Skin Care' THEN 'Yes' ELSE 'No' END AS Skin_Care_top_10,
    CASE WHEN top_10_percentile <= 1  AND custom_main_category = 'Hair Care' THEN 'Yes' ELSE 'No' END AS Hair_Care_top_10,
    CASE WHEN top_10_percentile <= 1  AND custom_main_category = 'Makeup' THEN 'Yes' ELSE 'No' END AS Makeup_top_10,

FROM
    StateRevenuePercentiles)


select 
customer_id,
state,
max(Skin_Care_top_10) as Skin_Care_top_10,
max(Hair_Care_top_10) as Hair_Care_top_10,
max(Makeup_top_10) as Makeup_top_10,

from final_tagging
where state != 'N/A'
group by All
