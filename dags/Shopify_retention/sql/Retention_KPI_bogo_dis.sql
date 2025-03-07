CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Retention_KPI_bogo_dis` AS 
WITH Ordercte AS (
    SELECT 
        DISTINCT
        customer_id,
        order_name,
        Datetime(Order_created_at, "Asia/Kolkata") as Order_created_at,
        Order_total_price, 
        Order_fulfillment_status, 
        order_financial_status,
        CASE 
            WHEN LOWER(COALESCE(discount_application, '') || COALESCE(discount_code, '')) LIKE '%b1g1%' THEN 'B1G1' 
            --WHEN LOWER(COALESCE(discount_application, '') || COALESCE(discount_code, '')) LIKE '%bogo%' THEN 'B1G1'
            WHEN LOWER(COALESCE(discount_application, '') || COALESCE(discount_code, '')) LIKE '%b2g2%' THEN 'B2G2'
            WHEN LOWER(COALESCE(discount_application, '') || COALESCE(discount_code, '')) LIKE '%b3g3%' THEN 'B3G3'
            WHEN LOWER(COALESCE(discount_application, '') || COALESCE(discount_code, '')) LIKE '%replacement%' THEN 'Replacement'
            ELSE LOWER(COALESCE(discount_application, '') || COALESCE(discount_code, '')) 
        END AS discount_final,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_created_at) AS order_rank
    FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` o  
    WHERE DATE(order_created_at) >= DATE(DATE_TRUNC(CURRENT_DATE, MONTH)- INTERVAL 1 MONTH - INTERVAL 13 MONTH) 
  --  WHERE 
  -- Order_fulfillment_status = 'fulfilled' 
  -- AND Order_financial_status NOT IN ('voided', 'refunded')
), 
revenue as (
    select date_trunc(order_created_at,month) as year_month, 
    sum(order_total_price) as total_revenue, 
    sum(case when order_rank>1 then order_total_price end) as rc_revenue 
    from ordercte  
    group by 1 
),
disc as(
SELECT date_trunc(order_created_at,month) as year_month, 
sum(order_total_price) as total_bogo_discount, 
sum(case when order_rank>1 then order_total_price end) as rc_dis_bogo
FROM Ordercte 
WHERE LOWER(discount_final) LIKE '%b1g1%' 
AND 
   Order_fulfillment_status = 'fulfilled' 
   AND Order_financial_status NOT IN ('voided', 'refunded')
group by 1 
order by year_month desc 
) 
select r.year_month,
d.total_bogo_discount, 
d.rc_dis_bogo, 
round((d.total_bogo_discount/r.total_revenue),4) as bogo_contri_per, 
round((d.rc_dis_bogo/r.rc_revenue),4) as repeats_bogo_contri_per

from revenue r inner join disc d on r.year_month=d.year_month  
order by r.year_month desc
