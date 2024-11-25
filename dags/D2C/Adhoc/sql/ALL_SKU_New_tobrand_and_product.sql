
with Shopify_orders as (select 
  customer_email as buyeremail,
  order_id,
  order_datetime,
  custom_parent_sku as sku,
  final_sale_revenue,
  quantity,
  dense_rank() over(partition by customer_email order by order_datetime) as Order_rank,
  dense_rank() over(partition by customer_email,custom_parent_sku order by order_datetime) as SKU_rank,
  -- case when custom_parent_sku like '%HGS%' then 1 else 0 end as bought_HGS

 FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
where channel = 'Shopify' and order_status not in ('cancelled','refunded') and customer_email !=''
),

final_tagging as 
(SELECT 
  buyeremail,
  order_id,
  order_datetime,
  sku,
  final_sale_revenue,
  quantity,
  Order_rank,
  SKU_rank,
  SUM(SKU_rank)
  OVER (
    PARTITION BY buyeremail,sku
    ORDER BY order_datetime
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS rolling_SKU_sum


from Shopify_orders
order by 1)


select

date_trunc(date(order_datetime),month) as year_month,
sku,
count(distinct buyeremail ) as Total_HGS_customer,
count(distinct case when order_rank=1 and SKU_rank=1 then buyeremail end) as First_time_to_brand_and_SKU,
count(distinct case when order_rank>1 and SKU_rank=1 then buyeremail end) as old_to_brand_first_time_SKU,
count(distinct case when SKU_rank>1  then buyeremail end) as SKU_repurchaser,

sum(distinct final_sale_revenue ) as Total_HGS_customer,
sum(distinct case when order_rank=1 and SKU_rank=1 then final_sale_revenue end) as First_time_to_brand_and_SKU,
sum(distinct case when order_rank>1 and SKU_rank=1 then final_sale_revenue end) as old_to_brand_first_time_SKU,
sum(distinct case when SKU_rank>1  then final_sale_revenue end) as SKU_repurchaser,


from final_tagging
where order_datetime>='2023-01-01'
group by ALL
order by 1

-- select * 
-- from final_tagging
-- order by 1,3,5

