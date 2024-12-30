with customer_base as
  (select
  customer_email as  customer_id,
  sum(final_sale_quantity) as total_qty,
  sum(custom_mrp*final_sale_quantity) as GMV_value,
  sum(final_sale_revenue) as Purchased_vales
  FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
  where customer_id not in ('','0') and order_status not in ('cancelled','refunded')
  and order_date>='2024-01-01' and channel = 'Shopify'
  group by ALL),

category_level as (
    SELECT  
    customer_email as customer_id,
    CASE 
      WHEN custom_main_category = 'Hair Care' THEN 'Hair Care'
      WHEN custom_main_category IN ('Lip Care', 'Face Care', 'Body Care') THEN 'Skin Care'
      WHEN custom_main_category = 'Makeup' THEN 'Makeup'
      ELSE 'Others'
    END AS category,
    custom_products,
    SUM(quantity) AS total_quantity,
    avg(custom_mrp) as MRP,
      FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
  where customer_id not in ('','0') and order_status not in ('cancelled','refunded')
  and order_date>='2024-01-01'
   and channel = 'Shopify'
  group by ALL
),

ranking_based as (
select 
customer_id,
category,
custom_products,
total_quantity,
MRP,
row_number() over(partition by customer_id,category order by total_quantity desc,MRP desc) as ranking,
row_number() over(partition by customer_id order by total_quantity desc,MRP desc) as overall_ranking

 from category_level
 where category in ('Hair Care','Skin Care','Makeup')
),


Hair_Care_base as  (
  select
  distinct
  customer_id,
  category,
  custom_products as Hair_Care_Product,
  from ranking_based
  where ranking = 1 and category = 'Hair Care'
),

Skin_Care_base as  (
  select
  distinct
  customer_id,
  category,
  custom_products as Skin_care_Product,
  from ranking_based
  where ranking = 1 and category = 'Skin Care'
),

Makeup_base as  (
  select
  distinct
  customer_id,
  category,
  custom_products as Makeup_Product,
  from ranking_based
  where ranking = 1 and category = 'Makeup'
),

Overall_base as  (
  select
  distinct
  customer_id,
  custom_products as Overall_Top_Product,
  from ranking_based
  where overall_ranking = 1 
)


select
distinct
  CB.customer_id,
  Overall_Top_Product,
  Hair_Care_Product,
  Skin_care_Product,
  Makeup_Product,
  total_qty,
  GMV_value,  
  Purchased_vales,
  (GMV_value-Purchased_vales) as Amount_Saved,
from customer_base as CB
left join Hair_Care_base as HC
using(customer_id)
left join Skin_Care_base as SC
using(customer_id)
left join Makeup_base as MC
using(customer_id)
left join Overall_base as O
using(customer_id)
