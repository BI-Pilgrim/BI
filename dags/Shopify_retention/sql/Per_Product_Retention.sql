CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.PPR_Retention_Main` AS 
WITH ordercte AS (
  SELECT DISTINCT customer_id,
         order_name,
         order_created_at,
  FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` 
  where Order_fulfillment_status = 'fulfilled' AND 
  Order_financial_status NOT in ('voided','refunded')
), 
item_sku_map AS (
  SELECT oi.customer_id, 
         oi.order_name, 
         oi.order_created_at,
         oi.item_sku_code,
         mt.master_sku as parent_sku,
         -- CASE WHEN mt.master_sku as parent_sku, --IN ('PGKS-AHGS1','PGKS-RAAHGSDPS1','PGGTM-RAAHGS1','PGKS-RAAHGS1') THEN 'ALL_HGS' 
         --ELSE mt.parent_sku END AS parent_sku, 
         --CASE WHEN mt.parent_sku IN ('PGKS-AHGS1','PGKS-RAAHGSDPS1','PGGTM-RAAHGS1','PGKS-RAAHGS1') THEN 'HAIR GROWTH SERUM' 
         --ELSE mt.product 
         mt.master_title as product_title,
         DENSE_RANK() OVER (PARTITION BY oi.customer_id ORDER BY oi.order_created_at) AS order_rank
  FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Order_items` oi
  LEFT JOIN (
    SELECT DISTINCT Master_Title, Master_SKU,Variant_ID-- AS INT) AS Variant_ID
    FROM `shopify-pubsub-project.Product_SKU_Mapping.D2C_SKU_mapping` where Type_of_Product in('Single') AND Parent_SKU not in('')
  ) mt ON oi.item_variant_id = mt.Variant_ID
), 
acquisition_cte AS (
  SELECT customer_id,
         order_name as first_order, 
         order_created_at AS acquisition_date,
         parent_sku AS first_product , 
         product_title as first_prod_title,
         order_rank
  FROM item_sku_map
  WHERE order_rank = 1
),

base as(
 select
distinct
B.customer_id,
B.order_rank,
B.order_name,
B.parent_sku,
--A.customer_id as repeated_id, 
B.order_created_at,
A.acquisition_date,
A.first_order,
A.first_product,
A.first_prod_title,
--FORMAT_TIMESTAMP('%Y%m', first_trans_date) AS cohort_year_month,
--FORMAT_TIMESTAMP('%Y%m', order_datetime) AS order_year_month,
DATE_DIFF(DATE(order_created_at),DATE(A.acquisition_date), DAY) as day_diff
from item_sku_map as B
inner join acquisition_cte as A
on A.customer_id = B.customer_id
and A.first_product = B.parent_sku
-- and B.order_name != first_order
order by B.customer_id
), 
Day_tagging as (
  select * , 
  CASE WHEN day_diff = 0 AND order_rank=1 THEN 1 ELSE 0 END AS Acq,
    CASE WHEN day_diff = 0 AND order_rank>1 THEN 1 ELSE 0 END AS D0,
    CASE WHEN day_diff <= 1 AND order_rank>1 THEN 1 ELSE 0 END AS D1,
    CASE WHEN day_diff <= 2 AND order_rank>1 THEN 1 ELSE 0 END AS D2,
    CASE WHEN day_diff <= 30 AND order_rank>1 THEN 1 ELSE 0 END AS D30,
    CASE WHEN day_diff <= 60 AND order_rank>1 THEN 1 ELSE 0 END AS D60,
    CASE WHEN day_diff <= 90 AND order_rank>1 THEN 1 ELSE 0 END AS D90, 
    CASE WHEN day_diff <= 120 AND order_rank>1 THEN 1 ELSE 0 END AS D120, 
    CASE WHEN day_diff <= 150 AND order_rank>1 THEN 1 ELSE 0 END AS D150, 
    CASE WHEN day_diff <= 180 AND order_rank>1 THEN 1 ELSE 0 END AS D180,
    CASE WHEN day_diff <= 270 AND order_rank>1 THEN 1 ELSE 0 END AS D270,
    CASE WHEN day_diff <= 365 AND order_rank>1 THEN 1 ELSE 0 END AS D360,
    CASE WHEN day_diff <= 395 AND order_rank>1 THEN 1 ELSE 0 END AS D390,
    CASE WHEN day_diff <=1500 AND order_rank>1 THEN 1 ELSE 0 END AS D360plus
   from base
), 
SKU_PPR_cohort AS (
  SELECT  
    DATE_TRUNC(DATE(acquisition_date), MONTH) AS Year_month,
    first_product,
    first_prod_title,
    COUNT(DISTINCT CASE WHEN Acq = 1 THEN customer_id END) AS Acquisition,
    COUNT(DISTINCT CASE WHEN D0 = 1 THEN customer_id END) AS D0,
    COUNT(DISTINCT CASE WHEN D1 = 1 THEN customer_id END) AS D1,
    COUNT(DISTINCT CASE WHEN D2 = 1 THEN customer_id END) AS D2,
    COUNT(DISTINCT CASE WHEN D30 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 30 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D30,
    COUNT(DISTINCT CASE WHEN D60 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 60 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D60,
    COUNT(DISTINCT CASE WHEN D90 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 90 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D90,
    COUNT(DISTINCT CASE WHEN D120 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 120 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D120,
    COUNT(DISTINCT CASE WHEN D150 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 150 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D150,
    COUNT(DISTINCT CASE WHEN D180 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 180 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D180,
    COUNT(DISTINCT CASE WHEN D270 = 1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 270 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D270,
    COUNT(DISTINCT CASE WHEN D360=1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 365 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D360, 
    COUNT(DISTINCT CASE WHEN D390=1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 395 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D390,
    COUNT(DISTINCT CASE WHEN D360plus=1 AND DATE_ADD(DATE(acquisition_date), INTERVAL 395 DAY) < CURRENT_DATE()-1 THEN customer_id END) AS D360plus
  FROM Day_tagging 
  WHERE DATE(acquisition_date) >= DATE(DATE_TRUNC(CURRENT_DATE, MONTH)- INTERVAL 2 MONTH - INTERVAL 2 YEAR) 
   -- AND DATE(acquisition_date) <= DATE_TRUNC(CURRENT_DATE, MONTH)
  GROUP BY 1, 2,3
  ORDER BY 1
)

select year_month, 
first_product,
first_prod_title, 
Acquisition, 
ROUND((D0/Acquisition),4) as D0, 
ROUND((D1/Acquisition),4) as D1, 
ROUND((D2/Acquisition),4) as D2, 
ROUND((D30/Acquisition),4) as D30,
ROUND((D60/Acquisition),4) as D60,
ROUND((D90/Acquisition),4) as D90,
ROUND((D120/Acquisition),4) as D120, 
ROUND((D150/Acquisition),4) as D150, 
ROUND((D180/Acquisition),4) as D180, 
ROUND((D270/Acquisition),4) as D270, 
ROUND((D360/Acquisition),4) as D360, 
ROUND((D390/Acquisition),4) as D390,
ROUND((D360plus/Acquisition),4) as D360plus
 from SKU_PPR_cohort s --inner join `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.sku_title_mapping` mp on s.first_product=mp.Parent_SKU
  --where customer_id = '6459455930597'  
  WHERE Acquisition>0
order by year_month desc
