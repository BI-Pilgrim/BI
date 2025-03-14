CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Retenton_KPI_Order_Count` AS
WITH Ordercte AS (
    SELECT 
        DISTINCT
        customer_id,
        order_name,
        DATETIME(Order_created_at, "Asia/Kolkata") as Order_created_at,
        Order_total_price,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_created_at) AS order_rank
    FROM shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders o 
    --WHERE 
    --    Order_fulfillment_status = 'fulfilled' 
    --    AND Order_financial_status NOT IN ('voided', 'refunded')
),

Acquisitioncte AS (
    SELECT 
        customer_id,
        order_name,
        order_created_at AS acquisition_date
    FROM Ordercte
    WHERE order_rank = 1 
), 

Base AS (
    SELECT 
        O.*,
        A.acquisition_date,
        DATE_DIFF(order_created_at, acquisition_date, DAY) AS day_diff
    FROM Ordercte AS O
    LEFT JOIN Acquisitioncte AS A
        ON O.customer_id = A.customer_id
), 

Day_tagging AS (
    SELECT 
        customer_id, 
        order_name,
        CASE WHEN day_diff = 0 AND order_rank = 1 THEN 1 ELSE 0 END AS Acq, 
        case when order_rank>1 then 1 else 0 end as repeated,
        CAST(order_created_at AS DATE) AS order_created_at, 
    FROM base  
    WHERE customer_id IS NOT NULL
   -- AND   customer_id = '6776931778789'
), 
ao_count as (
select 
DATE(DATE_TRUNC(order_created_at, MONTH)) AS year_month, 
COUNT(DISTINCT CASE WHEN Acq = 1 THEN customer_id END) AS Acquisition,
COUNT( distinct CASE WHEN repeated = 1 THEN customer_id END) AS RC,
COUNT(DISTINCT order_name) AS Total_order 
from day_tagging 
group by year_month 
),

Months AS (
    SELECT * 
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2024-01-01', CURRENT_DATE(), INTERVAL 1 MONTH)) AS month_start
), 
--select * from months

calc AS(  
    select
   m.month_start,  
    
   COUNT(DISTINCT CASE WHEN d.order_created_at < m.month_start THEN d.customer_id END) AS before_month_start,
COUNT(DISTINCT CASE WHEN d.order_created_at < DATE_TRUNC(DATE_ADD(m.month_start, INTERVAL 1 MONTH), MONTH) THEN d.customer_id END) AS after_month_end,


   COUNT(DISTINCT CASE WHEN d.order_created_at >= DATE_SUB(DATE(month_start), INTERVAL 12 MONTH) AND DATE(Order_created_at) < DATE(month_start) THEN d.customer_id END) AS before_12_month_start, 

  COUNT(DISTINCT CASE WHEN d.order_created_at >= DATE_SUB(DATE(DATE_ADD(DATE(month_start),INTERVAL 1 MONTH)), INTERVAL 12 MONTH) AND DATE(Order_created_at) < DATE_ADD(DATE(month_start), INTERVAL 1 MONTH) THEN d.customer_id END) AS after_12_month_end, 


   COUNT(DISTINCT CASE WHEN d.order_created_at >= DATE_SUB(DATE(month_start), INTERVAL 6 MONTH) AND DATE(Order_created_at) < DATE(month_start) THEN d.customer_id END) AS before_month_start_6, 

   COUNT(DISTINCT CASE WHEN d.order_created_at >= DATE_SUB(DATE(DATE_ADD(DATE(month_start),INTERVAL 1 MONTH)), INTERVAL 6 MONTH) AND DATE(Order_created_at) < DATE_ADD(DATE(month_start), INTERVAL 1 MONTH) THEN d.customer_id END) AS after_6_month_end 
    FROM 
        Months m 
    CROSS JOIN  Day_tagging d 
    GROUP BY month_start
) ,    

final_base as(
    SELECT 
    a.year_month, 
    a.acquisition,
    a.RC,
    a.total_order,
    c.before_month_start, 
    c.after_month_end, 
    c.before_12_month_start, 
    c.after_12_month_end,
    c.before_month_start_6, 
    c.after_6_month_end

    FROM calc c 
    inner join ao_count a on a.year_month = c.month_start
)
SELECT *, 
ROUND((RC /after_12_month_end), 5) AS Ret_m12, 
ROUND((RC/after_month_end), 4) AS Lifetime_Ret


FROM final_base
order by year_month desc

