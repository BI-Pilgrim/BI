   WITH successful_orders AS (
    SELECT 
        Customer_id,
        COUNT(DISTINCT CASE 
            WHEN DATETIME(TIMESTAMP(Order_created_at), 'Asia/Kolkata') >= DATETIME_SUB(DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata'), INTERVAL 6 MONTH) 
            THEN Order_id 
        END) AS orders_last_6_months,
        COUNT(DISTINCT Order_id) AS lifetime_orders,
        COUNT(DISTINCT CASE WHEN category = 'Skincare' THEN Order_id END) AS skincare_orders,
        COUNT(DISTINCT CASE WHEN category = 'Haircare' THEN Order_id END) AS haircare_orders
    FROM shopify-pubsub-project.adhoc_data_asia.temp_easycom2
    WHERE item_fulfillment_status = 'fulfilled'
    GROUP BY Customer_id
)
SELECT 
    te.Customer_id,
    te.Customer_first_name,
    te.Customer_last_name,
    te.Customer_phone,
    te.Customer_email,
    so.orders_last_6_months,
    so.lifetime_orders,
    so.skincare_orders,
    so.haircare_orders
FROM shopify-pubsub-project.adhoc_data_asia.temp_easycom2 te
JOIN successful_orders so ON te.Customer_id = so.Customer_id
WHERE 
    so.orders_last_6_months >= 10
    AND so.lifetime_orders >= 20
    AND so.skincare_orders >= 2
    AND so.haircare_orders >= 2;

-------------------------------------------------------------------------------------------------------
select distinct
    c.Customer_id,
    c.Customer_first_name,
    c.Customer_last_name,
    c.Customer_phone,
    c.Customer_email,
    o.item_fulfillment_status,
    o.Order_name,
    o.Order_id,
    o.item_sku_code,
    o.Order_created_at
FROM shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items o
LEFT JOIN
(select distinct customer_id,Customer_first_name,Customer_last_name,Customer_phone,Customer_email from shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Customers) c
ON c.Customer_id = o.customer_id
-- where cast(Order_created_at as date)>='2024-10-01'
-------------------------------------------------------------------------------------
# joining easycom to temp1(order_details+custmer) 
SELECT distinct
    t.Customer_id,
    t.Customer_first_name,
    t.Customer_last_name,
    t.Customer_phone,
    t.Customer_email,
    t.item_fulfillment_status,
    t.Order_name,
    t.Order_id,
    t.item_sku_code,
    t.Order_created_at,
    e.category
FROM shopify-pubsub-project.adhoc_data_asia.sku_190 e
LEFT JOIN shopify-pubsub-project.adhoc_data_asia.temp22 t 
    ON t.item_sku_code = e.sku;
