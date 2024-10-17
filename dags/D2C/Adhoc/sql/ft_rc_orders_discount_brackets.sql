WITH customer_orders AS (
  SELECT  
    c.Customer_id,
    o.Order_name,
    o.Order_created_at,
    o.discount_amount,
    o.total_line_items_price,
    o.Order_total_price,
    ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.Order_created_at ASC) AS order_sequence,
    -- CASE 
    --   WHEN c.customer_type = 'New' THEN 'FT'
    --   ELSE 'RC'
    -- END AS customer_type
  FROM `shopify-pubsub-project.Shopify_Production.Customer_Master` c
  LEFT JOIN `shopify-pubsub-project.Shopify_Production.Order_Master` o
    ON c.Customer_id = o.customer_id
  WHERE 
  -- DATE(TIMESTAMP(o.Order_created_at), "Asia/Kolkata") BETWEEN "2024-08-01" AND "2024-08-31"AND 
  o.Order_cancelled_reason IS NULL  -- Ensure only valid orders
),
tag as(
select *,
CASE 
      WHEN order_sequence = 1 THEN 'FT'
      ELSE 'RC'
    END AS customer_type
  from customer_orders),

discount_bracket AS (
  SELECT *,
    (discount_amount / total_line_items_price) * 100 AS discount_percentage,
    CASE 
      WHEN (discount_amount / total_line_items_price) * 100 = 0 THEN '0%'
      WHEN (discount_amount / total_line_items_price) * 100 BETWEEN 0.01 AND 10 THEN '0.01-10%'
      WHEN (discount_amount / total_line_items_price) * 100 BETWEEN 10.01 AND 15 THEN '10.01-15%'
      WHEN (discount_amount / total_line_items_price) * 100 BETWEEN 15.01 AND 20 THEN '15.01-20%'
      WHEN (discount_amount / total_line_items_price) * 100 BETWEEN 20.01 AND 30 THEN '20.01-30%'
      WHEN (discount_amount / total_line_items_price) * 100 BETWEEN 30.01 AND 40 THEN '30.01-40%'
      WHEN (discount_amount / total_line_items_price) * 100 BETWEEN 40.01 AND 50 THEN '40.01-50%'
      ELSE '>50%'
    END AS discounts
  FROM tag
)
SELECT 
  customer_type,
  discounts,
  COUNT(DISTINCT Order_name) AS total_orders,
  COUNT(DISTINCT customer_id) AS total_customers,
  SUM(Order_total_price) AS total_revenue
FROM discount_bracket
WHERE DATE(TIMESTAMP(Order_created_at), "Asia/Kolkata") BETWEEN "2024-08-01" AND "2024-08-31"
GROUP BY customer_type, discounts
ORDER BY customer_type, discounts;



