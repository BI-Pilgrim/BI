CREATE OR REPLACE TABLE `shopify-pubsub-project.adhoc_data_asia.order_duration_post_acq` AS
WITH order_ranks AS (
  SELECT 
    o.customer_id,
    c.customer_email,
    DATETIME(TIMESTAMP(o.Order_created_at), "Asia/Kolkata") AS order_time,
    o.landing_weburl,
    o.Order_source_name,
    o.discount_code,
    o.discount_amount,
    ROW_NUMBER() OVER (
      PARTITION BY o.customer_id ORDER BY TIMESTAMP(o.Order_created_at) ASC
    ) AS order_rank
  FROM 
    `Data_Warehouse_Shopify_Staging.Orders` o
  LEFT JOIN `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customers` c
  ON o.customer_id = c.Customer_id
  WHERE 
    TIMESTAMP_TRUNC(o.Order_created_at, DAY) >= TIMESTAMP("2024-10-01")
)
SELECT 
  a.customer_email,
  a.first_order,
  a.first_order_date,
  second_orders.second_order,
  rwoc.revised_type,
  -- Fetch the landing_weburl for first order using ARRAY_AGG and LIMIT 1
  a.first_order_landing_weburl,
  -- Fetch the landing_weburl for second order using ARRAY_AGG and LIMIT 1
  second_orders.second_order_landing_weburl,
  -- Calculate seconds difference between first_order and second_order
  CASE 
    WHEN second_orders.second_order IS NOT NULL 
    THEN TIMESTAMP_DIFF(second_orders.second_order, a.first_order, SECOND)
    ELSE NULL 
  END AS seconds_diff_first_second_order,
  -- Extract utm_source and utm_medium for first order
  REGEXP_EXTRACT(a.first_order_landing_weburl, r"utm_source=([^&]+)") AS first_order_utm_source,
  REGEXP_EXTRACT(a.first_order_landing_weburl, r"utm_medium=([^&]+)") AS first_order_utm_medium,
  -- Extract utm_source and utm_medium for second order
  REGEXP_EXTRACT(second_orders.second_order_landing_weburl, r"utm_source=([^&]+)") AS second_order_utm_source,
  REGEXP_EXTRACT(second_orders.second_order_landing_weburl, r"utm_medium=([^&]+)") AS second_order_utm_medium,
  -- Fetch the Order_source_name for first order using ARRAY_AGG and LIMIT 1
  a.order_source_name_first_order,
  -- Fetch the Order_source_name for second order using ARRAY_AGG and LIMIT 1
  second_orders.order_source_name_second_order,
  -- Fetch the discount_code for first order using ARRAY_AGG and LIMIT 1
  a.first_order_discount_code,
  -- Fetch the discount_code for second order using ARRAY_AGG and LIMIT 1
  second_orders.second_order_discount_code,
  -- Fetch the discount_amount for first order using ARRAY_AGG and LIMIT 1
  a.first_order_discount_amount,
  -- Fetch the discount_amount for second order using ARRAY_AGG and LIMIT 1
  second_orders.second_order_discount_amount
FROM 
  `shopify-pubsub-project.adhoc_data_asia.rwoc_customer` rwoc
INNER JOIN (
  -- Get First Order details including landing_weburl, Order_source_name, discount_code, discount_amount
  SELECT 
    customer_id,
    customer_email,
    MIN(order_time) AS first_order,
    MIN(DATE(order_time)) AS first_order_date,
    -- Aggregate the landing_weburl for first order using ARRAY_AGG and LIMIT 1
    ARRAY_AGG(landing_weburl ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS first_order_landing_weburl,
    -- Aggregate Order_source_name for first order
    ARRAY_AGG(Order_source_name ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS order_source_name_first_order,
    -- Aggregate discount_code for first order
    ARRAY_AGG(discount_code ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS first_order_discount_code,
    -- Aggregate discount_amount for first order
    ARRAY_AGG(discount_amount ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS first_order_discount_amount
  FROM 
    order_ranks
  WHERE 
    order_rank = 1
  GROUP BY 
    customer_id, customer_email
) a
ON LOWER(a.customer_email) = LOWER(rwoc.customer_email)
LEFT JOIN (
  -- Get Second Order details including landing_weburl, Order_source_name, discount_code, discount_amount
  SELECT 
    customer_id,
    customer_email,
    MIN(order_time) AS second_order,
    -- Aggregate the landing_weburl for second order using ARRAY_AGG and LIMIT 1
    ARRAY_AGG(landing_weburl ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS second_order_landing_weburl,
    -- Aggregate Order_source_name for second order
    ARRAY_AGG(Order_source_name ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS order_source_name_second_order,
    -- Aggregate discount_code for second order
    ARRAY_AGG(discount_code ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS second_order_discount_code,
    -- Aggregate discount_amount for second order
    ARRAY_AGG(discount_amount ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS second_order_discount_amount
  FROM 
    order_ranks
  WHERE 
    order_rank = 2
  GROUP BY 
    customer_id, customer_email
) second_orders
ON LOWER(a.customer_email) = LOWER(second_orders.customer_email)
WHERE 
  rwoc.customer_email IS NOT NULL
  AND (
    DATE_DIFF(DATE(a.first_order), rwoc.date_acq, DAY) <= 1 
    OR DATE_DIFF(rwoc.date_acq, DATE(a.first_order), DAY) <= 1
  )
GROUP BY 
ALL;
