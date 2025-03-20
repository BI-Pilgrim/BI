CREATE OR REPLACE TABLE `shopify-pubsub-project.Dashboard_category_pnl.tabletesting` AS
WITH easycom_data AS (
  SELECT
    FORMAT_TIMESTAMP('%b-%y', TIMESTAMP(invoice_date)) AS order_month,
    DATE(invoice_date) AS order_date,
    MP_Name AS marketplace,
    Order_Type AS order_type,
    Invoice_Status AS item_status,
    Component_SKU_Category AS category,
    Reference_Code,
    Item_Quantity,
    UPPER(REPLACE(Component_SKU, '`', '')) AS sku,
    INITCAP(Component_SKU_Name) AS productName,
    Sales_Channel,
    SUM(Taxable_Value + Tax) AS revenue,
    SUM(Tax) AS total_tax
  FROM `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Tax_report_new`
  WHERE Order_Status IN ('Shipped', 'Returned')
    AND report_type = 'TAX_REPORT_SALES'
    AND DATE(invoice_date) >= DATE '2025-01-01'
    AND DATE(invoice_date) < CURRENT_DATE()
    AND LOWER(Component_SKU) NOT LIKE '%tester%'
    AND LOWER(Component_SKU) NOT LIKE '%tstr%'
    AND LOWER(Component_SKU) NOT LIKE '%minis%'
    AND LOWER(Component_SKU) NOT LIKE '%sam%'
    AND LOWER(Component_SKU) NOT LIKE '%2mini%'
    AND LOWER(Component_SKU) NOT LIKE '%3mini%'
    AND LOWER(Component_SKU) NOT LIKE '%4mini%'
    AND Component_SKU_Name IS NOT NULL
    AND TRIM(Component_SKU_Name) <> ''
  GROUP BY ALL
),

normalized_data AS (
  SELECT
    *,
    CASE 
      WHEN productName LIKE '%Advanced Hair Growth Serum%' THEN 'Hair Growth Serum'
      WHEN productName LIKE '%Non-Drying Shampoo%' THEN 'Anti-dandruff Shampoo'
      WHEN productName LIKE '%Patua & Keratin Smoothening Shampoo%' THEN 'Patua & Keratin Shampoo'
      WHEN productName LIKE '%Patu & Keratin Strengthening Hair Mask%' THEN 'Patua & Keratin Hair Mask'
      WHEN productName LIKE '%Argan Oil Hair%' THEN 'Argan Oil Hair Serum'
      WHEN productName LIKE '%Vitamin C Face Serum%' THEN 'Vitamin C Face Serum'
      WHEN productName LIKE '%Niacinamide Face Serum%' THEN 'Niacinamide Face Serum'
      WHEN productName LIKE '%Squalane Moisturizer%' THEN 'Squalane Moisturizer'
      WHEN productName LIKE '%AHA BHA Peeling Solution%' THEN 'AHA BHA Peeling Solution'
      WHEN productName LIKE '%Alpha Arbutin & Vitamin C%' THEN 'Alpha Arbutin Serum'
      ELSE productName  
    END AS normalized_productName
  FROM easycom_data
),

npd_tracker AS (
  SELECT 
    UPPER(`SKU Code`) AS npd_sku,  
    DATE(`Launch Date`) AS launch_date,
    `Product SKU Name` AS npd_product_name
  FROM `shopify-pubsub-project.Dashboard_category_pnl.npd_sku_tracker`
),

npd_data AS (
  SELECT
    e.sku,
    e.order_month,
    e.order_date,
    DATE_TRUNC(order_date, WEEK) AS order_week, 
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM order_date) AS STRING), '-', EXTRACT(YEAR FROM order_date)) AS order_quarter,
    e.normalized_productName AS productName, 
    e.reference_code,
    e.Item_Quantity,
    CASE 
      WHEN e.category IN ('Haircare', 'Skincare', 'Makeup') THEN e.category
      ELSE 'Others'
    END AS category_group,
    e.marketplace AS marketplace_group,
    e.Sales_Channel,
    SUM(e.revenue) AS total_revenue,
    SUM(e.total_tax) AS total_tax,
    CASE 
      WHEN npd.launch_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH) THEN 'NPD'
      ELSE 'EPD'
    END AS npd_vs_epd
  FROM normalized_data e
  LEFT JOIN npd_tracker npd
    ON e.sku = npd.npd_sku
  GROUP BY ALL
),

returns_data AS (
  SELECT 
    REPLACE(Order_Number, '`', '') AS cleaned_Reference_Code,
    DATE_TRUNC(invoice_date, MONTH) AS year_month2,
    Marketplace, 
    Child_Sku,
    AVG(COALESCE(Total_Credit_Note_Amount, 0)) AS return_amount  
  FROM `shopify-pubsub-project.Data_Warehouse_Easyecom_Staging.Returns_report` 
  WHERE Marketplace in('Shopify','Offline')
  GROUP BY ALL
),

last_30_days_data AS (
  SELECT 
    sku,
   -- marketplace_group,
    --sales_channel,
    SUM(Item_Quantity) AS last_30_days_quantity
  FROM npd_data
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY all
),

ranked_data AS (
  SELECT t.*,
       COALESCE(r.return_amount, 0) AS return_amount,
       ROW_NUMBER() OVER (PARTITION BY order_month, category_group, marketplace_group, npd_vs_epd 
                          ORDER BY total_revenue DESC) AS overall_rank,
       SUM(total_revenue) OVER (PARTITION BY order_month, category_group, marketplace_group, npd_vs_epd 
                                ORDER BY total_revenue DESC 
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 
       NULLIF(SUM(CASE WHEN total_revenue > 0 THEN total_revenue ELSE 0 END) 
              OVER (PARTITION BY order_month, category_group, marketplace_group, npd_vs_epd), 0) 
       AS cumulative_contribution
FROM npd_data t
LEFT JOIN returns_data r 
    ON t.Reference_Code = r.cleaned_Reference_Code
    AND t.sku = r.Child_Sku
 
 -- where total_revenue >0
),

result_with_contribution1 AS (
  SELECT 
    r.*,
    COALESCE(l30.last_30_days_quantity, 0) / 30 AS DRR_VIEW
  FROM ranked_data r
  LEFT JOIN last_30_days_data l30 ON r.sku = l30.sku
),
-- select * from result_with_contribution1

result AS (
  SELECT 
    *,  
    CASE 
      WHEN npd_vs_epd = 'EPD' AND overall_rank <= 5 THEN 'EPD_Top' 
      WHEN npd_vs_epd = 'EPD' AND overall_rank > 5 THEN 'EPD_Tail' 
      WHEN npd_vs_epd = 'NPD' AND cumulative_contribution <= 0.30 THEN 'NPD_Top_30%'
      WHEN npd_vs_epd = 'NPD' AND cumulative_contribution > 0.30 THEN 'NPD_Tail'
      ELSE 'others' 
    END AS Head_Tail_Tagging,
    CASE 
      WHEN marketplace_group = 'Shopify' AND Sales_Channel = '`Marketplace B2C' THEN 'Shopify'
      WHEN marketplace_group <> 'Shopify' AND Sales_Channel = '`Marketplace B2C' THEN 'All_marketplace_b2c'
      WHEN Sales_Channel IN ('`Nykaa','`Big Basket', 
                             '`Zepto', '`Avni-B2B', '`RK-World', '`Reliance', '`Swiggy','`FirstCry','`Blink It','`Purplle','`Myntra-Jabong') 
           AND marketplace_group != 'Shopify' THEN 'All_marketplace_b2b'
      WHEN Sales_Channel IN ('`GT','`GT-BA','`GT-NON BA','`MT','`MT BA','`MT Core') THEN 'Offline'
      ELSE 'others' 
    END AS channel,
  CASE 
    WHEN marketplace_group = 'Firstcry' AND sales_channel = '`Marketplace B2C' THEN 'Firstcry'
    WHEN marketplace_group = 'Shopify' AND sales_channel = '`Marketplace B2C' THEN 'Shopify'
    WHEN marketplace_group = 'Snapdeal' AND sales_channel = '`Marketplace B2C' THEN 'Snapdeal'
    WHEN marketplace_group = 'Smytten' AND sales_channel = '`Marketplace B2C' THEN 'Smytten'
    WHEN marketplace_group = 'Flipkart' AND sales_channel = '`Marketplace B2C' THEN 'Flipkart'
    WHEN marketplace_group = 'Amazon.in' AND sales_channel = '`Marketplace B2C' THEN 'Amazon.in'
    WHEN marketplace_group = 'Amazon_FBA' AND sales_channel = '`Marketplace B2C' THEN 'Amazon_FBA'
    WHEN marketplace_group = 'Myntra PPMP' AND sales_channel = '`Marketplace B2C' THEN 'Myntra PPMP'
    ELSE Sales_Channel
END AS Sales_Channel1

    -- case
    -- when marketplace_group in ('Myntra PPMP','Smytten','Flipkart','Shopify','Snapdeal','Firstcry','Amazon.in','Amazon_FBA') and sales_channel='`Marketplace B2C' then 'All_markeplace_B2C' 
    -- else Sales_Channel
    -- end as Sales_Channel
   
  FROM result_with_contribution1
), 

last_90_days_data AS (
  SELECT 
    npd_vs_epd,
    SUM(total_revenue) AS total_revenue_90d 
  FROM result
  WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) 
    AND channel IN ('offline', 'Shopify', 'All_marketplace_b2b','All_marketplace_b2c')
  GROUP BY npd_vs_epd
), 
result_with_contribution2 AS (
  SELECT 
    r.*,
    --COALESCE(l90.total_revenue_90d, 0) AS total_revenue_90d,
    --COALESCE(l30.last_30_days_quantity, 0) / 30 AS DRR_VIEW,
    CASE 
      WHEN r.npd_vs_epd = 'EPD' THEN (r.total_revenue / NULLIF(l90.total_revenue_90d, 0)) * 100
      WHEN r.npd_vs_epd = 'NPD' THEN (r.total_revenue / NULLIF(l90.total_revenue_90d, 0)) * 100
      ELSE 0
    END AS product_wise_contribution
  FROM result r
  LEFT JOIN last_90_days_data l90 ON r.npd_vs_epd = l90.npd_vs_epd
  LEFT JOIN last_30_days_data l30 ON r.sku = l30.sku
),

 cte_check AS (
  SELECT 
    sku, 
    order_month, order_week, order_quarter, order_date, productName, 
    Sales_Channel1, r.channel, category_group AS Category, 
    mp.Sub_Category, mp.Concern, mp.mrp_og, npd_vs_epd, 
    Head_Tail_Tagging AS EPD_NPD, Item_Quantity, 
    DRR_VIEW ,
    COUNT(DISTINCT reference_code) AS count_order,
    SUM(total_revenue) AS total_revenue, 
    SUM(total_revenue - total_tax) AS gross_without_gst, 
    SUM(
      CASE 
        WHEN r.channel IN ('Shopify','Offline')
        THEN (total_revenue - total_tax - return_amount)
        ELSE 0 
      END
    ) AS net_Sales, 

    SUM(mp.mrp_og * item_quantity) AS GMV,
    --SUM(1 - (total_revenue / (mp.mrp_og * item_quantity))) AS Discount,

--    -- Category-level total sales
--   sum(SUM(total_revenue)) OVER (PARTITION BY order_date,sales_channel1,sku) AS category_sales, 
--
--    -- Sub-category-level total sales
--   sum(SUM(total_revenue))   OVER (PARTITION BY order_date,mp.Sub_Category,sales_channel1,sku) AS sub_category_sales, 
--
--    -- Overall total sales
--   sum(SUM(total_revenue))  OVER (partition by order_date,sales_channel1,category_group) AS total_sales,
--
--    -- Product contribution calculations in percentage
--    -- (SUM(total_revenue) / NULLIF(SUM(SUM(total_revenue)) OVER (PARTITION BY category_group), 0)) * 100 AS --product_contribution_category,
--    -- (SUM(total_revenue) / NULLIF(SUM(SUM(total_revenue)) OVER (PARTITION BY mp.Sub_Category), 0)) * 100 AS --product_contribution_sub_cat,
--    -- (SUM(total_revenue) / NULLIF(SUM(SUM(total_revenue)) OVER (), 0)) * 100 AS product_contribution_overall,


  FROM result_with_contribution2 r 
  LEFT JOIN (
    WITH RankedRows AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY parent_sku ORDER BY concern) AS row_num
      FROM `shopify-pubsub-project.Product_SKU_Mapping.Master_SKU_mapping`
    ) 
    SELECT * FROM RankedRows WHERE row_num = 1
  ) mp ON r.sku = mp.parent_sku

  WHERE r.channel IN ('Offline', 'Shopify', 'All_marketplace_b2b','All_marketplace_b2c') 
  GROUP BY ALL 
), 
sales_category as(
  select 
  --sku,
  sales_channel1,
  order_date,
  category,
  sum(gross_without_gst) as sales_category
  from cte_check 
  group by all
), 
sales_subcategory as(
  select 
  sales_channel1, 
  order_date, 
  category,
  sub_category, 
  sum(gross_without_gst) as subcategory_sales 
  from cte_check 
  group by all
),
--select * from sales_category

-- select channel,sum(gross_without_gst) from cte_check where order_month='Jan-25' group by all 
cte_join as (
select  c.*,d.sales_category
from  cte_check c 
left join sales_category d on 
c.order_date = d.order_date AND 
c.sales_channel1 = d.sales_channel1 AND 
c.category = d.category 
--where order_month='Jan-25' and channel='Shopify'  and c.category='Skincare' and  sku = 'PGA-10VCFS30' and gross_without_gst>0 
group by all 
) 
select c.*, d.subcategory_sales 
from cte_join c 
left join sales_subcategory d on 
c.order_date = d.order_date AND 
c.sales_channel1 = d.sales_channel1 AND 
c.category = d.category AND 
c.sub_category = d.sub_category 
group by all