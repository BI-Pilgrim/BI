CREATE OR  REPLACE TABLE `shopify-pubsub-project.Retention_Cohort.Retention_KPI_crm_metrics` AS
WITH Ordercte AS (
    SELECT 
        DISTINCT
        customer_id,
        order_name,
        Datetime(Order_created_at, "Asia/Kolkata") as Order_created_at,
        Order_total_price, 
        REGEXP_EXTRACT(landing_weburl, r"utm_source=([^&]+)") AS utm_source, 
        case when Order_source_name='web' then 'web'
        when Order_source_name is not null then 'app'
        else 'NA' end as Order_source_name,
        Order_fulfillment_status,
        Order_financial_status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_created_at) AS order_rank
    FROM `shopify-pubsub-project.Data_Warehouse_Shopify_Staging.Orders` o 
),

Acquisitioncte AS (
    SELECT 
        customer_id,
        order_name,
        order_created_at AS acquisition_date
    FROM Ordercte
    WHERE order_rank = 1 
),

Base AS (
    SELECT 
        O.*,
        A.acquisition_date,
        DATE_DIFF(order_created_at, acquisition_date, DAY) AS day_diff
    FROM Ordercte AS O
    LEFT JOIN Acquisitioncte AS A
        ON O.customer_id = A.customer_id
),

Day_tagging AS (
    SELECT 
        *, 
        CASE WHEN day_diff = 0 AND order_rank = 1 THEN 1 ELSE 0 END AS Acq,
        case when order_rank>1 then 1 else 0 end as repeated, 
        from base
), 
retention as(
select
date(date_trunc(order_created_at,month)) as year_month, 
COUNT( distinct CASE WHEN repeated = 1 THEN customer_id END) AS RC, 
count(distinct case when repeated=1 then order_name end) as Order_count,
count(distinct case when repeated=1 AND Lower(utm_source) like '%crm%' then order_name end) as Order_count_crm, 
count(distinct case when repeated=1 AND Lower(utm_source) like '%google' then order_name end) as Order_count_google, 
count(distinct case when repeated=1 AND Lower(utm_source) like '%fb%' OR Lower(utm_source) like '%facebook%' then order_name end) as Order_count_fb, 
count(distinct case when repeated=1 AND Lower(order_source_name) like '%app%' then order_name end) as Order_count_app

from Day_tagging  
where date(order_created_at)>='2022-06-1'
--WHERE 
  --Order_fulfillment_status = 'fulfilled' 
--AND Order_financial_status NOT IN ('voided', 'refunded') 
group by 1 
),
other_count as(
select  
year_month,
Order_count_crm, 
Order_count_google,
Order_count_fb,
Order_count_app,  
(Order_count - 
        (Order_count_crm + 
         Order_count_google + 
         Order_count_fb + 
         Order_count_app)) AS Order_count_others, 

  ROUND(Order_count_crm / Order_count, 4) AS Percentage_CRM,
  ROUND(Order_count_google / Order_count, 4) AS Percentage_Google,
  ROUND(Order_count_fb / Order_count, 4) AS Percentage_FB,
  ROUND(Order_count_app / Order_count, 4) AS Percentage_App
 from retention   
) 
select *, 

    from other_count 
    order by year_month desc

--where year_month='2024-12-01'