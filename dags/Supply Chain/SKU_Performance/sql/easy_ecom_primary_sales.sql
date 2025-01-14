CREATE OR REPLACE TABLE adhoc_data_asia.easyecom_sku_sales_l90 AS
SELECT
CONCAT(
    EXTRACT(YEAR FROM oi.order_date), "-", 
    LPAD(CAST(EXTRACT(MONTH FROM oi.order_date) AS STRING), 2, "0")
  ) AS order_month,
  DATE(oi.order_date) AS order_date,
  marketplace,
  o.location_key,
  l.location_name,
  order_type,
  item_status,
  category,
  UPPER(oi.sku) AS sku,
  INITCAP(oi.productName) AS productName,
  CAST(SUM(COALESCE(item_quantity,0)) as INT64) as item_quantity,
  ROUND(SUM(COALESCE(selling_price,0)),2) as revenue
FROM
`shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orderitem` oi
LEFT JOIN `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Orders` o
ON oi.order_id=o.order_id
LEFT JOIN (select distinct location_key,location_name from `easycom.locations`) l
on l.location_key=o.location_key
WHERE oi.order_date>=current_date - interval '90' day and o.order_date>=current_date - interval '90' day
GROUP BY ALL
ORDER BY order_date DESC
