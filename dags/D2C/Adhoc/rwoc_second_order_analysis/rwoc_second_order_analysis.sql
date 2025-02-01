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
    o.discount_application, -- New field added
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
  a.first_order_landing_weburl,
  second_orders.second_order_landing_weburl,
  CASE 
    WHEN second_orders.second_order IS NOT NULL 
    THEN TIMESTAMP_DIFF(second_orders.second_order, a.first_order, SECOND)
    ELSE NULL 
  END AS seconds_diff_first_second_order,
  REGEXP_EXTRACT(a.first_order_landing_weburl, r"utm_source=([^&]+)") AS first_order_utm_source,
  REGEXP_EXTRACT(a.first_order_landing_weburl, r"utm_medium=([^&]+)") AS first_order_utm_medium,
  REGEXP_EXTRACT(second_orders.second_order_landing_weburl, r"utm_source=([^&]+)") AS second_order_utm_source,
  REGEXP_EXTRACT(second_orders.second_order_landing_weburl, r"utm_medium=([^&]+)") AS second_order_utm_medium,
  a.order_source_name_first_order,
  second_orders.order_source_name_second_order,
  a.first_order_discount_code,
  second_orders.second_order_discount_code,
  a.first_order_discount_amount,
  second_orders.second_order_discount_amount,
  a.first_order_discount_application, -- New column added
  second_orders.second_order_discount_application -- New column added
FROM 
  `shopify-pubsub-project.adhoc_data_asia.rwoc_customer` rwoc
INNER JOIN (
  SELECT 
    customer_id,
    customer_email,
    MIN(order_time) AS first_order,
    MIN(DATE(order_time)) AS first_order_date,
    ARRAY_AGG(landing_weburl ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS first_order_landing_weburl,
    ARRAY_AGG(Order_source_name ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS order_source_name_first_order,
    ARRAY_AGG(discount_code ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS first_order_discount_code,
    ARRAY_AGG(discount_amount ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS first_order_discount_amount,
    ARRAY_AGG(discount_application ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS first_order_discount_application -- New column added
  FROM 
    order_ranks
  WHERE 
    order_rank = 1
  GROUP BY 
    customer_id, customer_email
) a
ON LOWER(a.customer_email) = LOWER(rwoc.customer_email)
LEFT JOIN (
  SELECT 
    customer_id,
    customer_email,
    MIN(order_time) AS second_order,
    ARRAY_AGG(landing_weburl ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS second_order_landing_weburl,
    ARRAY_AGG(Order_source_name ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS order_source_name_second_order,
    ARRAY_AGG(discount_code ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS second_order_discount_code,
    ARRAY_AGG(discount_amount ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS second_order_discount_amount,
    ARRAY_AGG(discount_application ORDER BY order_time ASC LIMIT 1)[OFFSET(0)] AS second_order_discount_application -- New column added
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
