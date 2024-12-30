
WITH Customer_State_Sales AS
(
    select 
  customer_email as customer_id,
  case when ship_state in ('Uttarakhand','UTTRAKHAND') then 'Uttarakhand'
  when ship_state in ('Tamilnadu','Tamil Nadu') then 'Tamil Nadu'
  when ship_state in ('Bihar state','Bihar') then 'Bihar'
  else ship_state
  end as state,
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
    total_qty
  from Customer_State_Sales as A
  left join customer_state_mapping as B
  using(customer_id)


),

StateRevenuePercentiles AS (
    SELECT
        state,
        customer_id,
        total_qty,
        NTILE(100) OVER (PARTITION BY state ORDER BY total_qty DESC) AS revenue_percentile,
        NTILE(10) OVER (PARTITION BY state ORDER BY total_qty DESC) AS top_10_percentile,
        NTILE(5) OVER (PARTITION BY state ORDER BY total_qty DESC) AS top_20_percentile,
        NTILE(2) OVER (PARTITION BY state ORDER BY total_qty DESC) AS top_50_percentile
    FROM
        base
),
final_base as (
SELECT
    distinct
    state,
    customer_id,
    CASE WHEN revenue_percentile <= 1 THEN 'Yes' ELSE 'No' END AS top_1_percentile,
    CASE WHEN top_10_percentile <= 1 THEN 'Yes' ELSE 'No' END AS top_10_percentile,
    CASE WHEN top_20_percentile <= 1 THEN 'Yes' ELSE 'No' END AS top_20_percentile,
    CASE WHEN top_50_percentile <= 1 THEN 'Yes' ELSE 'No' END AS top_50_percentile
FROM
    StateRevenuePercentiles
)

select 
state,
customer_id,
max(top_1_percentile) as top_1_percentile,
max(top_10_percentile) as top_10_percentile,
max(top_20_percentile) as top_20_percentile,
max(top_50_percentile) as top_50_percentile,
from final_base
group by all
