WITH CustomerQuarterlySales AS
(
    select 
  date_trunc(order_date,quarter) as quarter,
  customer_email as  customer_id,
  sum(final_sale_quantity) as total_qty
  FROM `pilgrim-dw.halo_115.global_reports_project_level_report_order_items` 
  where customer_id not in ('','0') and order_status not in ('cancelled','refunded')
  and order_date>='2024-01-01' and channel = 'Shopify'
  group by All
),

CustomerQuarterlyRanking AS (
    SELECT
        distinct
        customer_id,
        quarter,
        total_qty,
        NTILE(10) OVER (PARTITION BY quarter ORDER BY total_qty DESC) AS qty_percentile
    FROM
        CustomerQuarterlySales
),
final_base as 
(SELECT
    distinct
    customer_id,
    CASE
        WHEN qty_percentile <= 1 and quarter = '2024-01-01' THEN 'Yes'
        ELSE 'No'
    END AS JFM24,
    
    CASE
        WHEN qty_percentile <= 1 and quarter = '2024-04-01' THEN 'Yes'
        ELSE 'No'
    END AS AMJ24,

    
    CASE
        WHEN qty_percentile <= 1 and quarter = '2024-07-01' THEN 'Yes'
        ELSE 'No'
    END AS JAS24,

    
    CASE
        WHEN qty_percentile <= 1 and quarter = '2024-10-01' THEN 'Yes'
        ELSE 'No'
    END AS OND24
FROM
    CustomerQuarterlyRanking)

    select 
    customer_id,
    max(JFM24) as JFM24,
    max(AMJ24) as AMJ24,
    max(JAS24) as JAS24,
    max(OND24) as OND24,
    from final_base
    group by 1

