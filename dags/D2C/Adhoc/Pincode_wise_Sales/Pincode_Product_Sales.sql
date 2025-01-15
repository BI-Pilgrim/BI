-- Pincode <> SKU level sales.. If a Pincode lies in 2 states, one with higher sale is attributed to the pincode

CREATE OR REPLACE TABLE `adhoc_data_asia.shopify_pincode_sales` AS
WITH product_data AS (
  -- Extract product name and price from the JSON field 'line_items'
  SELECT
    name AS order_name,  -- Use 'name' as order_name in product_data CTE
    JSON_EXTRACT_SCALAR(item, '$.name') AS product_name,
    CAST(JSON_EXTRACT_SCALAR(item, '$.price') AS FLOAT64) * CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS INT64) AS total_price
  FROM
    `shopify-pubsub-project.pilgrim_bi_airbyte.orders`,
    UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS item  -- Flatten the line_items array
  WHERE
    TIMESTAMP_TRUNC(TIMESTAMP(DATETIME(created_at, "Asia/Kolkata")), DAY) >= TIMESTAMP(CURRENT_DATE() - INTERVAL 90 DAY)
),
order_data AS (
  -- Calculate total sales by order, shipping_zip, and shipping_province
  SELECT
    order_name,  -- This is 'order_name' in shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders
    shipping_zip,
    shipping_province,
    SUM(Order_total_price) AS Order_total_price
  FROM
    `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders`
  WHERE
    TIMESTAMP_TRUNC(TIMESTAMP(DATETIME(Order_created_at, "Asia/Kolkata")), DAY) >= TIMESTAMP(CURRENT_DATE() - INTERVAL 90 DAY)
  GROUP BY
    order_name, shipping_zip, shipping_province
),
pincode_sales AS (
  -- Aggregate total sales per pincode and shipping_province
  SELECT
    shipping_zip,
    shipping_province,
    SUM(Order_total_price) AS total_sales
  FROM
    order_data
  GROUP BY
    shipping_zip, shipping_province
),
ranked_provinces AS (
  -- Rank shipping provinces by total sales within each pincode
  SELECT
    shipping_zip,
    shipping_province,
    total_sales,
    RANK() OVER (PARTITION BY shipping_zip ORDER BY total_sales DESC) AS province_rank
  FROM
    pincode_sales
),
total_revenue_per_pincode AS (
  -- Calculate the total revenue per pincode
  SELECT
    shipping_zip,
    SUM(Order_total_price) AS total_pincode_revenue
  FROM
    order_data
  GROUP BY
    shipping_zip
)

-- Final selection and table creation without revenue rank or revenue contribution
SELECT
  od.shipping_zip,
  od.shipping_province,
  pd.product_name,
  SUM(pd.total_price) AS Order_total_price
FROM
  order_data od
LEFT JOIN
  product_data pd ON od.order_name = pd.order_name
JOIN
  ranked_provinces rp ON od.shipping_zip = rp.shipping_zip AND od.shipping_province = rp.shipping_province
JOIN
  total_revenue_per_pincode trp ON od.shipping_zip = trp.shipping_zip
GROUP BY
  od.shipping_zip, od.shipping_province, pd.product_name
ORDER BY
  od.shipping_zip, od.shipping_province, product_name;


-- Pincode <> SKU level sales contribution

CREATE OR REPLACE TABLE `adhoc_data_asia.shopify_pincode_sales_contribution` AS
SELECT prank.*, 
       (prank.Order_total_price / psales.pincode_sales) * 100 AS pct_contribution
FROM (
  SELECT *, 
         RANK() OVER (PARTITION BY shipping_zip ORDER BY Order_total_price DESC) AS revenue_rank
  FROM `adhoc_data_asia.shopify_pincode_sales`
) prank
LEFT JOIN (
  SELECT shipping_zip, 
         SUM(Order_total_price) AS pincode_sales
  FROM `adhoc_data_asia.shopify_pincode_sales`
  GROUP BY shipping_zip
) AS psales
ON prank.shipping_zip = psales.shipping_zip
ORDER BY prank.Order_total_price DESC;



-- Final table with pincode rankings as well

CREATE OR REPLACE TABLE `adhoc_data_asia.shopify_pincode_sales_contribution_best_pincode` AS
SELECT 
    psales.*, 
    c.product_pct_contribution AS pincode_contribution,
    pincode_rank
    --RANK() OVER (ORDER BY b.product_pct_contribution DESC) AS pincode_rank
FROM 
    `adhoc_data_asia.shopify_pincode_sales_contribution` psales
LEFT JOIN (
    select *,RANK() OVER (ORDER BY b.product_pct_contribution DESC) AS pincode_rank from
    (SELECT 
        trank.shipping_zip, 
        round((trank.Order_total_price / tsales.total_sales) * 100,5) AS product_pct_contribution,
        --RANK() OVER (ORDER BY b.product_pct_contribution DESC) AS pincode_rank
    FROM (
        SELECT 
            shipping_zip, 
            SUM(Order_total_price) AS Order_total_price
        FROM 
            `adhoc_data_asia.shopify_pincode_sales_contribution`
        GROUP BY 
            shipping_zip
    ) trank
    CROSS JOIN (
        SELECT
            SUM(Order_total_price) AS total_sales
        FROM 
            `adhoc_data_asia.shopify_pincode_sales_contribution`
    ) tsales
) b )c
ON psales.shipping_zip = c.shipping_zip;

