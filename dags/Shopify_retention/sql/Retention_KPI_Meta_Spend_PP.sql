CREATE OR REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Retention_KPI_Meta_spend_pp` AS 
SELECT date(date_trunc(date_start,month) ) AS month_start, 
sum(spend) as total_spend
FROM `shopify-pubsub-project.Data_Warehouse_Facebook_Ads_Staging.ads_insights_normal` 
where ad_name like 'PP |%' and DATE(date_start) >= DATE(DATE_TRUNC(CURRENT_DATE, MONTH)- INTERVAL 1 MONTH - INTERVAL 13 MONTH)
group by 1 
order by 1 desc