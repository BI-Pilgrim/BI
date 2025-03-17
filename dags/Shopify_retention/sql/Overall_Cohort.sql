CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Overall_Retention_Main` AS
WITH Ordercte AS (
    SELECT 
        DISTINCT
        customer_id,
        order_name,
        order_created_at,
        Order_total_price,
        Order_fulfillment_status,
        Order_financial_status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_created_at) AS order_rank
    FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` o 
   -- WHERE 
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
        *,
        CASE WHEN day_diff = 0 AND order_rank = 1 THEN 1 ELSE 0 END AS Acq,
        CASE WHEN day_diff = 0 AND order_rank > 1 THEN 1 ELSE 0 END AS D0,
        CASE WHEN day_diff <= 1 AND order_rank > 1 THEN 1 ELSE 0 END AS D1,
        CASE WHEN day_diff <= 2 AND order_rank > 1 THEN 1 ELSE 0 END AS D2,
        CASE WHEN day_diff <= 30 AND order_rank > 1 THEN 1 ELSE 0 END AS D30,
        CASE WHEN day_diff <= 60 AND order_rank > 1 THEN 1 ELSE 0 END AS D60,
        CASE WHEN day_diff <= 90 AND order_rank > 1 THEN 1 ELSE 0 END AS D90, 
        CASE WHEN day_diff <= 120 AND order_rank > 1 THEN 1 ELSE 0 END AS D120, 
        CASE WHEN day_diff <= 150 AND order_rank > 1 THEN 1 ELSE 0 END AS D150, 
        CASE WHEN day_diff <= 180 AND order_rank > 1 THEN 1 ELSE 0 END AS D180,
        CASE WHEN day_diff <= 270 AND order_rank > 1 THEN 1 ELSE 0 END AS D270,
        CASE WHEN day_diff <= 365 AND order_rank > 1 THEN 1 ELSE 0 END AS D360,
        CASE WHEN day_diff <= 395 AND order_rank > 1 THEN 1 ELSE 0 END AS D390,
        CASE WHEN day_diff <= 1500 AND order_rank > 1 THEN 1 ELSE 0 END AS D360plus
    FROM Base 
    WHERE 
        Order_fulfillment_status = 'fulfilled' 
        AND Order_financial_status NOT IN ('voided', 'refunded')
),

customer_cohort AS (
    SELECT 
        DATE(DATE_TRUNC(acquisition_date, MONTH)) AS year_month,
        COUNT(DISTINCT CASE WHEN Acq = 1 THEN customer_id END) AS Acquisition,
        COUNT(DISTINCT CASE WHEN D0 = 1 THEN customer_id END) AS D0_c,
        COUNT(DISTINCT CASE WHEN D1 = 1 THEN customer_id END) AS D1_c,
        COUNT(DISTINCT CASE WHEN D2 = 1 THEN customer_id END) AS D2_c,
        COUNT(DISTINCT CASE WHEN D30 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 30 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D30_c,
        COUNT(DISTINCT CASE WHEN D60 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 60 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D60_c,
        COUNT(DISTINCT CASE WHEN D90 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 90 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D90_c,
        COUNT(DISTINCT CASE WHEN D120 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 120 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D120_c,
        COUNT(DISTINCT CASE WHEN D150 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 150 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D150_c,
        COUNT(DISTINCT CASE WHEN D180 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 180 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D180_c,
        COUNT(DISTINCT CASE WHEN D270 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 270 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D270_c,
        COUNT(DISTINCT CASE WHEN D360 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 365 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D360_c,
        COUNT(DISTINCT CASE WHEN D390 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 395 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D390_c, 
        COUNT(DISTINCT CASE WHEN D360plus = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 395 DAY) < CURRENT_DATE() - 1 THEN customer_id END) AS D360plus_c, 

        COUNT(DISTINCT CASE WHEN Acq = 1 THEN order_name END) AS Total_order,
        COUNT(DISTINCT CASE WHEN D0 = 1 THEN order_name END) AS D0_O,
        COUNT(DISTINCT CASE WHEN D1 = 1 THEN order_name END) AS D1_O,
        COUNT(DISTINCT CASE WHEN D2 = 1 THEN order_name END) AS D2_O,
        COUNT(DISTINCT CASE WHEN D30 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 30 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D30_O,
        COUNT(DISTINCT CASE WHEN D60 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 60 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D60_O,
        COUNT(DISTINCT CASE WHEN D90 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 90 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D90_O,
        COUNT(DISTINCT CASE WHEN D120 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 120 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D120_O,
        COUNT(DISTINCT CASE WHEN D150 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 150 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D150_O,
        COUNT(DISTINCT CASE WHEN D180 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 180 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D180_O,
        COUNT(DISTINCT CASE WHEN D270 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 270 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D270_O,
        COUNT(DISTINCT CASE WHEN D360 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 365 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D360_O,
        COUNT(DISTINCT CASE WHEN D390 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 395 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D390_O,
        COUNT(DISTINCT CASE WHEN D360plus = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 395 DAY) < CURRENT_DATE() - 1 THEN order_name END) AS D360plus_O,

        
        SUM(CASE WHEN Acq = 1 THEN order_total_price END) AS total_revenue,
        SUM(CASE WHEN D0 = 1 THEN order_total_price END) AS D0_rev,
        SUM(CASE WHEN D1 = 1 THEN order_total_price END) AS D1_rev,
        SUM(CASE WHEN D2 = 1 THEN order_total_price END) AS D2_rev,
        SUM(CASE WHEN D30 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 30 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D30_rev,
        SUM(CASE WHEN D60 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 60 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D60_rev,
        SUM(CASE WHEN D90 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 90 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D90_rev,
        SUM(CASE WHEN D120 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 120 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D120_rev,
        SUM(CASE WHEN D150 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 150 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D150_rev,
        SUM(CASE WHEN D180 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 180 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D180_rev,
        SUM(CASE WHEN D270 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 270 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D270_rev,
        SUM(CASE WHEN D360 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 365 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D360_rev, 
        SUM(CASE WHEN D390 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 395 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D390_rev,
        SUM(CASE WHEN D360plus = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 395 DAY) < CURRENT_DATE() - 1 THEN order_total_price END) AS D360plus_rev
    FROM Day_tagging
    WHERE 
        DATE(acquisition_date) >= DATE(DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 2 MONTH - INTERVAL 2 YEAR) 
       -- AND DATE(acquisition_date) <= DATE_TRUNC(CURRENT_DATE(), MONTH) - 1
    GROUP BY year_month
    ORDER BY year_month
),

customercte as(
SELECT 
    year_month, 
    Acquisition, 
    'Customer' AS metric,
    ROUND((D0_c / Acquisition), 4) AS D0, 
    ROUND((D1_c / Acquisition), 4) AS D1,
    ROUND((D2_c / Acquisition), 4) AS D2,
    ROUND((D30_c / Acquisition), 4) AS D30, 
    ROUND((D60_c / Acquisition), 4) AS D60, 
    ROUND((D90_c / Acquisition), 4) AS D90, 
    ROUND((D120_c / Acquisition), 4) AS D120, 
    ROUND((D150_c / Acquisition), 4) AS D150, 
    ROUND((D180_c / Acquisition), 4) AS D180, 
    ROUND((D270_c / Acquisition), 4) AS D270, 
    ROUND((D360_c / Acquisition), 4) AS D360, 
    ROUND((D390_c / Acquisition), 4) AS D390,
    ROUND((D360plus_c / Acquisition), 4) AS D390_plus,  
FROM customer_cohort
), 
revenuecte as(
SELECT
    year_month,  
    ROUND(total_revenue / 10000000, 2) AS NC_Revenue_in_cr, 
    'Revenue' AS metric,  
    ROUND((D0_rev / total_revenue), 4) AS D0, 
    ROUND((D1_rev / total_revenue), 4) AS D1,
    ROUND((D2_rev / total_revenue), 4) AS D2,
    ROUND((D30_rev / total_revenue), 4) AS D30, 
    ROUND((D60_rev / total_revenue), 4) AS D60, 
    ROUND((D90_rev / total_revenue), 4) AS D90, 
    ROUND((D120_rev / total_revenue), 4) AS D120, 
    ROUND((D150_rev / total_revenue), 4) AS D150, 
    ROUND((D180_rev / total_revenue), 4) AS D180, 
    ROUND((D270_rev / total_revenue), 4) AS D270, 
    ROUND((D360_rev / total_revenue), 4) AS D360, 
    ROUND((D390_rev / total_revenue), 4) AS D390,
    ROUND((D360plus_rev / total_revenue), 4) AS D390_plus, 
    FROM customer_cohort
), 
total_order_cte as( 
SELECT
year_month, 
total_order,
'Order' AS metric,
ROUND((D0_O / Total_order), 4) AS D0,  
ROUND((D1_O / Total_order), 4) AS D1,
ROUND((D2_O / Total_order), 4) AS D2,
ROUND((D30_O /  Total_order), 4) AS D30, 
ROUND((D60_O /  Total_order), 4) AS D60, 
ROUND((D90_O /  Total_order), 4) AS D90, 
ROUND((D120_O / Total_order), 4) AS D120, 
ROUND((D150_O / Total_order), 4) AS D150, 
ROUND((D180_O / Total_order), 4) AS D180, 
ROUND((D270_O / Total_order), 4) AS D270, 
ROUND((D360_O / Total_order), 4) AS D360, 
ROUND((D390_O / Total_order), 4) AS D390,
ROUND((D360plus_O / Total_order), 4) AS D360_plus 
    from customer_cohort
) 
SELECT * FROM customercte
UNION ALL
SELECT * FROM revenuecte
UNION ALL
SELECT * FROM total_order_cte

