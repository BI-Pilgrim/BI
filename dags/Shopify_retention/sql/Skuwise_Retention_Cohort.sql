CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Overall_Skuwise_Retention_Main` AS
WITH ordercte AS (
  SELECT DISTINCT customer_id,
         order_name,
         order_created_at,
         order_total_price
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
--select * from item_sku_map where customer_id = '8228486906085'
acquisition_cte AS (
  SELECT customer_id,
         order_name, 
         order_created_at AS acquisition_date,
         parent_sku AS acquired_sku ,
         order_rank, 
         product_title
         --item_sku_code AS acquired_sku 
  FROM item_sku_map
  WHERE order_rank = 1
),
base AS (
  SELECT DISTINCT oc.customer_id, 
         oc.order_name,
         oc.order_created_at,
         oc.order_total_price,
         --act.parent_sku,
         act.product_title,
         act.acquired_sku, 
         itm.order_rank,
         act.acquisition_date,
         DATE_DIFF(oc.order_created_at, acquisition_date, DAY) AS day_diff
  FROM acquisition_cte AS act
  RIGHT JOIN ordercte AS oc USING (customer_id)
  LEFT JOIN item_sku_map AS itm
    ON oc.customer_id = itm.customer_id
    AND oc.order_name = itm.order_name
), 
--select * from base where customer_id = '8228486906085' 
Day_tagging as (
  select  
    customer_id, 
    product_title,
    order_name, 
    order_rank, 
    order_created_at,
    acquired_sku, 
    acquisition_date,
    order_total_price,
    case when day_diff = 0 and order_rank=1 then 1 else 0 end as Acq,
    case when day_diff = 0 and order_rank>1 then 1 else 0 end as D0,
    case when day_diff <= 1 and order_rank>1 then 1 else 0 end as D1,
    case when day_diff <= 2 and order_rank>1 then 1 else 0 end as D2,
    case when day_diff <= 30 and order_rank>1 then 1 else 0 end as D30,
    case when day_diff <= 60 and order_rank>1 then 1 else 0 end as D60,
    case when day_diff <= 90 and order_rank>1 then 1 else 0 end as D90, 
    case when day_diff <= 120 and order_rank>1 then 1 else 0 end as D120, 
    case when day_diff <= 150 and order_rank>1 then 1 else 0 end as D150, 
    case when day_diff <= 180 and order_rank>1 then 1 else 0 end as D180,
    case when day_diff <= 270 and order_rank>1 then 1 else 0 end as D270,
    case when day_diff <= 365 and order_rank>1 then 1 else 0 end as D360, 
    case when day_diff <= 395 and order_rank>1 then 1 else 0 end as D390,
    case when day_diff <=1500 and order_rank>1 then 1 else 0 end as D360plus
  from base
), 
SKU_cohort as(
select  
date_trunc(date(Day_tagging.acquisition_date),Month) as Year_month, 
product_title,
Acquired_sku, 

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





from Day_tagging 
where DATE(acquisition_date) >= DATE(DATE_TRUNC(CURRENT_DATE, MONTH)- INTERVAL 1 MONTH - INTERVAL 13 MONTH) 
  
group by 1,2,3
order by 1
),   
--select * from sku_cohort 
customercte as(
select year_month, 
acquired_sku,
Product_Title, 
Acquisition, 
'Customer' AS metric, 
ROUND((D0_c / Acquisition), 4) AS D0_C, 
ROUND((D1_c / Acquisition), 4) AS D1_C,
ROUND((D2_c / Acquisition), 4) AS D2_C,
ROUND((D30_c / Acquisition), 4) AS D30_C, 
ROUND((D60_c / Acquisition), 4) AS D60_C, 
ROUND((D90_c / Acquisition), 4) AS D90_C, 
ROUND((D120_c / Acquisition), 4) AS D120_C, 
ROUND((D150_c / Acquisition), 4) AS D150_C, 
ROUND((D180_c / Acquisition), 4) AS D180_C, 
ROUND((D270_c / Acquisition), 4) AS D270_C, 
ROUND((D360_c / Acquisition), 4) AS D360_C, 
ROUND((D390_c / Acquisition), 4) AS D390_C,
ROUND((D360plus_c / Acquisition), 4) AS D360_plu_C,  
    from sku_cohort --s inner join `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.sku_title_mapping` mp on s.acquired_sku=mp.Parent_SKU 
WHERE Acquisition>0 AND Total_order>0 and total_revenue>0
), 
revenuecte as(
select year_month, 
acquired_sku,
Product_Title,
ROUND(total_revenue / 10000000, 2) AS NC_Revenue_in_cr, 
'Revenue' AS metric,
ROUND((D0_rev / total_revenue), 4) AS D0_rev, 
ROUND((D1_rev / total_revenue), 4) AS D1_rev,
ROUND((D2_rev / total_revenue), 4) AS D2_rev,
ROUND((D30_rev / total_revenue), 4) AS D30_rev, 
ROUND((D60_rev / total_revenue), 4) AS D60_rev, 
ROUND((D90_rev / total_revenue), 4) AS D90_rev, 
ROUND((D120_rev / total_revenue), 4) AS D120_rev, 
ROUND((D150_rev / total_revenue), 4) AS D150_rev, 
ROUND((D180_rev / total_revenue), 4) AS D180_rev, 
ROUND((D270_rev / total_revenue), 4) AS D270_rev, 
ROUND((D360_rev / total_revenue), 4) AS D360_rev,
ROUND((D390_rev / total_revenue), 4) AS D390_rev, 
ROUND((D360plus_rev / total_revenue), 4) AS D360_plu_rev, 
from sku_cohort --s inner join `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.sku_title_mapping` mp on s.acquired_sku=mp.Parent_SKU 
WHERE Acquisition>0 AND Total_order>0 and total_revenue>0
),
total_order_cte as (
select year_month, 
acquired_sku,
Product_Title, 
Total_order, 
'Order' AS metric,     
ROUND((D0_O / Total_order), 4) AS D0_O, 
ROUND((D1_O / Total_order), 4) AS D1_O,
ROUND((D2_O / Total_order), 4) AS D2_O,
ROUND((D30_O /  Total_order), 4) AS D30_O, 
ROUND((D60_O /  Total_order), 4) AS D60_O, 
ROUND((D90_O /  Total_order), 4) AS D90_O, 
ROUND((D120_O / Total_order), 4) AS D120_O, 
ROUND((D150_O / Total_order), 4) AS D150_O, 
ROUND((D180_O / Total_order), 4) AS D180_O, 
ROUND((D270_O / Total_order), 4) AS D270_O, 
ROUND((D360_O / Total_order), 4) AS D360_O, 
ROUND((D390_O / Total_order), 4) AS D390_O,
ROUND((D360plus_O / Total_order), 4) AS D360_plu_O    
from sku_cohort s --inner join `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.sku_title_mapping` mp on s.acquired_sku=mp.Parent_SKU 
WHERE Acquisition>0 AND Total_order>0 and total_revenue>0
 ), 
  -- s inner join item_sku_map mp on s.acquired_sku = mp.parent_sku --WHERE customer_id = '6459455930597';  
cte_check as(
SELECT * FROM customercte
UNION ALL
SELECT * FROM revenuecte
UNION ALL
SELECT * FROM total_order_cte
) 
select * from cte_check where acquired_sku is not null --AND Product_title = 'Bullet Lipstick'--AND acquired_sku = 'PGKABPPS1'
