CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Retention_KPI_M0Cont` AS
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
        DATE_DIFF(DATE(order_created_at),DATE(acquisition_date), MONTH) AS month_diff, 
        DATE_DIFF(order_created_at, acquisition_date, DAY) AS day_diff

    FROM Ordercte AS O
    LEFT JOIN Acquisitioncte AS A
        ON O.customer_id = A.customer_id
),

Day_tagging AS (
    SELECT 
        *,
        CASE WHEN day_diff = 0 AND order_rank = 1 THEN 1 ELSE 0 END AS Acq,
        CASE WHEN month_diff = 0 AND order_rank > 1 THEN 1 ELSE 0 END AS M0,
        case when order_rank>1 then 1 else 0 end as repeated
    FROM Base 
WHERE 
  Order_fulfillment_status = 'fulfilled' 
  AND Order_financial_status NOT IN ('voided', 'refunded') 
),

customer_cohort AS (
    SELECT 
        DATE(DATE_TRUNC(order_created_at, MONTH)) AS year_month,
        SUM( CASE WHEN M0 = 1 AND DATE_TRUNC(date(acquisition_date),month) <= date_trunc(CURRENT_DATE(),month) THEN order_total_price END) AS M0_cont,
        SUM(CASE WHEN repeated = 1  THEN order_total_price END) AS RC_revenue 
    FROM Day_tagging 
    WHERE DATE(order_created_at) >= DATE(DATE_TRUNC(CURRENT_DATE, MONTH)- INTERVAL 1 MONTH - INTERVAL 13 MONTH) 
    GROUP BY year_month
    ORDER BY year_month desc
)

SELECT  *, 
ROUND((M0_cont/RC_revenue),4) AS M0_Contri_Per
FROM customer_cohort
ORDER BY year_month desc;
