CREATE OR  REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Retention_KPI` AS
WITH Ordercte AS (
    SELECT 
        DISTINCT
        customer_id,
        order_name,
        Datetime(Order_created_at, "Asia/Kolkata") as Order_created_at,
        Order_total_price,
        Order_fulfillment_status,
        Order_financial_status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_created_at) AS order_rank
    FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` o 
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
        *, 
        case when Order_created_at = acquisition_date then 'New' else 'Repeat' end as New_Repeat_Tag,
        CASE WHEN day_diff = 0 AND order_rank = 1 THEN 1 ELSE 0 END AS Acq,
        case when order_rank>1 then 1 else 0 end as repeated,
    FROM Base
), 


retention as(
select
date(date_trunc(order_created_at,month)) as year_month, 
COUNT( distinct CASE WHEN acq = 1 THEN customer_id END) AS NC,
COUNT( distinct CASE WHEN repeated = 1 THEN customer_id END) AS RC, 
count(distinct case when repeated=1 then order_name end) as Order_count, 
SUM(CASE WHEN repeated = 1 or acq=1  THEN order_total_price END) AS Total_revenue,
SUM(CASE WHEN repeated = 1  THEN order_total_price END) AS RC_revenue 

from Day_tagging 
WHERE 
   Order_fulfillment_status = 'fulfilled' 
   AND Order_financial_status NOT IN ('voided', 'refunded') 
   AND DATE(order_created_at) >=DATE(DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 MONTH - INTERVAL 13 MONTH)
group by 1 
)  

select *, 
ROUND((RC_revenue /Order_count), 2) AS AOV_RC, 
ROUND((Order_count/RC),2) as RC_order_per_C,  
ROUND((RC_revenue /Total_revenue)*100, 2) AS rc_revenue_contri, 
ROUND((RC_revenue/RC),2) as arpu

 from retention  
order by year_month desc
