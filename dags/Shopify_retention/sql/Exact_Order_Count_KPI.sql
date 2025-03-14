CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Retention_KPI_Exact_Order_Count` as
WITH cust AS (
    SELECT  
        c.customer_id, 
        c.Customer_created_at,
        o.order_name, 
        o.Order_created_at
    FROM 
        `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customers` c
    LEFT JOIN 
        `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` o 
        ON o.customer_id = c.customer_id
    WHERE 
        c.customer_id IS NOT NULL
), 

zero_order_counts AS (
    SELECT
        customer_id,
        LAST_DAY(DATE(DATE_TRUNC(customer_created_at, MONTH))) AS year_month,
        COUNT(DISTINCT order_name) AS order_count
    FROM 
        cust
    GROUP BY 
        customer_id, year_month
), 
monthly_zero_order_counts AS (
    SELECT
        year_month,
        COUNT(CASE WHEN order_count = 0 THEN 1 END) AS zero_orders,
    FROM 
        zero_order_counts
    GROUP BY 
        year_month
),
--SELECT * FROM monthly_zero_order_counts 
cumulative_zero_counts AS (
    SELECT
        DATE_TRUNC(year_month, MONTH) AS year_month,
        SUM(zero_orders) OVER (ORDER BY year_month ) AS cumulative_zero_orders,
    FROM
        monthly_zero_order_counts 

),
--SELECT *  FROM cumulative_zero_counts
Months AS (
    SELECT 
    LAST_DAY(DATE_ADD(DATE '2024-01-31', INTERVAL x MONTH), MONTH) AS year_month
  FROM UNNEST(GENERATE_ARRAY(0, DATE_DIFF(LAST_DAY(CURRENT_DATE(), MONTH), DATE '2022-03-31', MONTH), 1)) AS x
),  
orders AS (
    SELECT  
        year_month,
        customer_id,
        COUNT(DISTINCT order_name) AS order_count
    FROM cust AS c
    CROSS JOIN Months AS m 
    WHERE DATE(Order_created_at) <= year_month
    GROUP BY year_month, customer_id
),
cummultative_count as(
SELECT 
    DATE_TRUNC(year_month, MONTH) AS year_month,
    COUNT(DISTINCT CASE WHEN order_count = 1 THEN customer_id END) AS cumulative_one_order, 
    COUNT(DISTINCT CASE WHEN order_count = 2 THEN customer_id END) AS cumulative_two_order, 
    COUNT(DISTINCT CASE WHEN order_count = 3 THEN customer_id END) AS cumulative_three_order, 
    COUNT(DISTINCT CASE WHEN order_count > 3 THEN customer_id END) AS cumulative_three_plus_order,
FROM orders
GROUP BY year_month
--ORDER BY year_month 
), 
zero_all as(
    select 
    c.year_month, 
    co.cumulative_zero_orders, 
    c.cumulative_one_order, 
    c.cumulative_two_order, 
    c.cumulative_three_order, 
    c.cumulative_three_plus_order
    from cummultative_count c inner join cumulative_zero_counts co on DATE_TRUNC(c.year_month,MONTH) = co.year_month
), 
--select * from zero_all
total_orders_by_month AS (
    SELECT
        year_month,
        (SUM(cumulative_zero_orders) + SUM(cumulative_one_order) + SUM(cumulative_two_order) + SUM(cumulative_three_order) + SUM(cumulative_three_plus_order)) AS total_orders
    FROM
        zero_all
    GROUP BY 
        year_month
) 
SELECT
    DATE_TRUNC(c.year_month,MONTH) AS year_month,
    c.cumulative_zero_orders, 
    c.cumulative_one_order, 
    c.cumulative_two_order,
    c.cumulative_three_order, 
    c.cumulative_three_plus_order,
    
    --t.total_orders,
    CASE WHEN t.total_orders != 0
    THEN (c.cumulative_zero_orders / t.total_orders) 
    ELSE 0 END AS contribution_zero_orders,

    CASE WHEN t.total_orders != 0
    THEN (c.cumulative_one_order / t.total_orders) 
    ELSE 0 END AS contribution_one_order,

    CASE WHEN t.total_orders != 0
    THEN (c.cumulative_two_order / t.total_orders) 
    ELSE 0 END AS contribution_two_orders,

    CASE WHEN t.total_orders != 0
    THEN (c.cumulative_three_order / t.total_orders) 
    ELSE 0 END AS contribution_three_orders,

    CASE WHEN t.total_orders != 0
    THEN (c.cumulative_three_plus_order / t.total_orders) 
    ELSE 0 END AS contribution_three_or_more_orders

FROM 
    zero_all c
JOIN 
    total_orders_by_month t ON DATE_TRUNC(c.year_month,MONTH) = DATE_TRUNC(t.year_month,MONTH)
    WHERE c.year_month <= DATE_TRUNC(c.year_month,MONTH) 
ORDER BY 
    c.year_month DESC; 
