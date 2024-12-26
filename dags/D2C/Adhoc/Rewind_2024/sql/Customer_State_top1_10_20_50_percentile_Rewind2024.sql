WITH Customer_State_Sales AS
(
    select 
  date_trunc(order_date,year) as year,
  customer_email as customer_id,
  CASE
      WHEN lower(SHIP_STATE) LIKE '%andaman%' THEN 'Andaman & Nicobar'
      WHEN lower(SHIP_STATE) LIKE '%andh%' THEN 'Andhra Pradesh'
      WHEN lower(SHIP_STATE) LIKE '%aruna%' THEN 'Arunachal Pradesh'
      WHEN lower(SHIP_STATE) LIKE '%assa%' THEN 'Assam'
      WHEN lower(SHIP_STATE) LIKE '%bihar%' THEN 'Bihar'
      WHEN lower(SHIP_STATE) LIKE '%chandigarh%' THEN 'Chandigarh'
      WHEN lower(SHIP_STATE) LIKE '%chhat%' THEN 'Chhattisgarh'
      WHEN lower(SHIP_STATE) LIKE '%dadra%' THEN 'Dadra & Nagar Haveli'
      WHEN lower(SHIP_STATE) LIKE '%daman%' THEN 'Daman and Diu'
      WHEN lower(SHIP_STATE) LIKE '%delhi%' THEN 'Delhi'
      WHEN lower(SHIP_STATE) LIKE '%goa%' THEN 'Goa'
      WHEN lower(SHIP_STATE) LIKE '%guj%' THEN 'Gujarat'
      WHEN lower(SHIP_STATE) LIKE '%har%' THEN 'Haryana'
      WHEN lower(SHIP_STATE) LIKE '%himachal%' THEN 'Himachal Pradesh'
      WHEN lower(SHIP_STATE) LIKE '%jammu%' THEN 'Jammu & Kashmir'
      WHEN lower(SHIP_STATE) LIKE '%jhark%' THEN 'Jharkhand'
      WHEN lower(SHIP_STATE) LIKE '%karna%' THEN 'Karnataka'
      WHEN lower(SHIP_STATE) LIKE '%kera%' THEN 'Kerala'
      WHEN lower(SHIP_STATE) LIKE '%ladakh%' THEN 'Ladakh'
      WHEN lower(SHIP_STATE) LIKE '%madhya%' THEN 'Madhya Pradesh'
      WHEN lower(SHIP_STATE) LIKE '%mahara%' THEN 'Maharashtra'
      WHEN lower(SHIP_STATE) LIKE '%manip%' THEN 'Manipur'
      WHEN lower(SHIP_STATE) LIKE '%meghalaya%' THEN 'Meghalaya'
      WHEN lower(SHIP_STATE) LIKE '%mizo%' THEN 'Mizoram'
      WHEN lower(SHIP_STATE) LIKE '%naga%' THEN 'Nagaland'
      WHEN lower(SHIP_STATE) LIKE '%odi%' THEN 'Odisha'
      WHEN lower(SHIP_STATE) LIKE '%pondi%' OR lower(SHIP_STATE) LIKE '%pud%' THEN 'Puducherry'
      WHEN lower(SHIP_STATE) LIKE '%punj%' THEN 'Punjab'
      WHEN lower(SHIP_STATE) LIKE '%raj%' THEN 'Rajasthan'
      WHEN lower(SHIP_STATE) LIKE '%sikkim%' THEN 'Sikkim'
      WHEN lower(SHIP_STATE) LIKE '%tamil%' THEN 'Tamil Nadu'
      WHEN lower(SHIP_STATE) LIKE '%telang%' THEN 'Telangana'
      WHEN lower(SHIP_STATE) LIKE '%trip%' THEN 'Tripura'
      WHEN lower(SHIP_STATE) LIKE '%uttar%prad%' THEN 'Uttar Pradesh'
      WHEN lower(SHIP_STATE) LIKE '%uttarak%' THEN 'Uttarakhand'
      WHEN lower(SHIP_STATE) LIKE '%bengal%' THEN 'West Bengal'
      ELSE 'Others'
    END AS state,
  sum(final_sale_quantity) as total_qty
  FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
  where customer_id not in ('','0') and order_status not in ('cancelled','refunded')
  and order_date>='2024-01-01'
  group by All
),

StateRevenuePercentiles AS (
    SELECT
        state,
        customer_id,
        total_qty,
        NTILE(100) OVER (PARTITION BY state ORDER BY total_qty DESC) AS revenue_percentile,
        NTILE(10) OVER (PARTITION BY state ORDER BY total_qty DESC) AS top_10_percentile,
        NTILE(5) OVER (PARTITION BY state ORDER BY total_qty DESC) AS top_20_percentile,
        NTILE(2) OVER (PARTITION BY state ORDER BY total_qty DESC) AS top_50_percentile
    FROM
        Customer_State_Sales
)
SELECT
    distinct
    state,
    customer_id,
    CASE WHEN revenue_percentile <= 1 THEN 'Yes' ELSE 'No' END AS top_1_percentile,
    CASE WHEN top_10_percentile <= 1 THEN 'Yes' ELSE 'No' END AS top_10_percentile,
    CASE WHEN top_20_percentile <= 1 THEN 'Yes' ELSE 'No' END AS top_20_percentile,
    CASE WHEN top_50_percentile <= 1 THEN 'Yes' ELSE 'No' END AS top_50_percentile
FROM
    StateRevenuePercentiles;
