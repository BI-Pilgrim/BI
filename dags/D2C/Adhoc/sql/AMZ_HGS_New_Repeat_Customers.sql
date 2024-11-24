
with amazon_orders as (select 
  buyeremail,
  order_id,
  order_datetime,
  sku,
  final_sale_revenue,
  quantity,
  dense_rank() over(partition by buyeremail order by order_datetime) as Order_rank,
  case when sku like '%HGS%' then 1 else 0 end as bought_HGS

 FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
where channel = 'amazon seller partner' and buyeremail != ''
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
  bought_HGS,
  SUM(bought_HGS)
  OVER (
    PARTITION BY buyeremail
    ORDER BY order_datetime
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS rolling_HGS_sum


from amazon_orders
order by 1)


select

date_trunc(date(order_datetime),month) as year_month,

count(distinct case when bought_HGS = 1 then buyeremail end) as Total_HGS_customer,
count(distinct case when order_rank=1 and bought_HGS=1 then buyeremail end) as First_time_to_brand_and_product,
count(distinct case when order_rank>1 and bought_HGS=1 and rolling_HGS_sum = 1 then buyeremail end) as old_to_brand_first_time_HGS,
count(distinct case when order_rank>1 and bought_HGS=1 and rolling_HGS_sum >1 then buyeremail end) as HGS_repurchaser,

sum(case when order_rank=1 and bought_HGS=1 then final_sale_revenue end) as First_time_to_brand_and_product_R,
sum(case when order_rank>1 and bought_HGS=1 and rolling_HGS_sum = 1 then final_sale_revenue end) as old_to_brand_first_time_HGS_R,
sum(case when order_rank>1 and bought_HGS=1 and rolling_HGS_sum >1 then final_sale_revenue end) as HGS_repurchaser_R

from final_tagging
group by ALL
order by 1
