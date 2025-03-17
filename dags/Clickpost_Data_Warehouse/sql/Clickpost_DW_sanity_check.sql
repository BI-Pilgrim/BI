CREATE OR REPLACE TABLE `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.sanity` AS
WITH sources AS (
    SELECT 
        'Orders' AS table_name,
        'orders' AS source_table,
        MAX(DATE(`Ingestion Date`)) AS Source_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(`Ingestion Date`) = (SELECT MAX(DATE(Ingestion_Date)) 
                                                FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Orders`) 
                THEN CONCAT(`Order ID`,`Reference Number`) 
            END
        ) AS Source_pk_count
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.orders`

    UNION ALL

    SELECT 
        'Addresses' AS table_name,
        'addresses' AS source_table,
        MAX(DATE(`Ingestion Date`)) AS Source_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(`Ingestion Date`) = (SELECT MAX(DATE(Ingestion_Date)) 
                                                FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Addresses`) 
                THEN `Order ID` 
            END
        ) AS Source_pk_count
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.addresses` 

    UNION ALL

    SELECT 
        'Package' AS table_name,
        'package' AS source_table,
        MAX(DATE(`Ingestion Date`)) AS Source_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(`Ingestion Date`) = (SELECT MAX(DATE(Ingestion_Date)) 
                                                FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Package`) 
                THEN CONCAT(`Order ID`,`SKU`) 
            END
        ) AS Source_pk_count
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.package` 

    UNION ALL 

    SELECT 
        'Shipping' AS table_name,
        'shipping' AS source_table,
        MAX(DATE(`Ingestion Date`)) AS Source_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(`Ingestion Date`) = (SELECT MAX(DATE(Ingestion_Date)) 
                                                FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Shipping`) 
                THEN CONCAT(`Order ID`,`AWB`) 
            END
        ) AS Source_pk_count
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.shipping` 

    UNION ALL

    SELECT 
        'Tracking' AS table_name,
        'tracking' AS source_table,
        MAX(DATE(`Ingestion Date`)) AS Source_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(`Ingestion Date`) = (SELECT MAX(DATE(Ingestion_Date)) 
                                                FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Tracking`) 
                THEN `Order ID` 
            END
        ) AS Source_pk_count
    FROM `shopify-pubsub-project.pilgrim_bi_clickpost.tracking`
), 

staging AS (
    SELECT 
        'Orders' AS table_name,
        MAX(DATE(Ingestion_Date)) AS Staging_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(Ingestion_Date) = (SELECT MAX(DATE(`Ingestion Date`)) 
                                                FROM `shopify-pubsub-project.pilgrim_bi_clickpost.orders`) 
                THEN CONCAT(Reference_Number) 
            END
        ) AS Staging_pk_count
    FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Orders`

    UNION ALL

    SELECT 
        'Addresses' AS table_name,
        MAX(DATE(Ingestion_Date)) AS Staging_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(Ingestion_Date) = (SELECT MAX(DATE(`Ingestion Date`)) 
                                                FROM `shopify-pubsub-project.pilgrim_bi_clickpost.addresses`) 
                THEN Order_ID 
            END
        ) AS Staging_pk_count
    FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Addresses` 

    UNION ALL 

    SELECT 
        'Package' AS table_name,
        MAX(DATE(Ingestion_Date)) AS Staging_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(Ingestion_Date) = (SELECT MAX(DATE(`Ingestion Date`)) 
                                                FROM `shopify-pubsub-project.pilgrim_bi_clickpost.package`) 
                THEN CONCAT(Order_ID,SKU) 
            END
        ) AS Staging_pk_count
    FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Package` 

    UNION ALL 

    SELECT 
        'Shipping' AS table_name,
        MAX(DATE(Ingestion_Date)) AS Staging_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(Ingestion_Date) = (SELECT MAX(DATE(`Ingestion Date`)) 
                                                FROM `shopify-pubsub-project.pilgrim_bi_clickpost.shipping`) 
                THEN CONCAT(Order_ID,AWB) 
            END
        ) AS Staging_pk_count
    FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Shipping` 

    UNION ALL

    SELECT 
        'Tracking' AS table_name,
        MAX(DATE(Ingestion_Date)) AS Staging_max_date,
        COUNT(DISTINCT 
            CASE 
                WHEN DATE(Ingestion_Date) = (SELECT MAX(DATE(`Ingestion Date`)) 
                                                FROM `shopify-pubsub-project.pilgrim_bi_clickpost.tracking`) 
                THEN Order_ID 
            END
        ) AS Staging_pk_count
    FROM `shopify-pubsub-project.Data_Warehouse_ClickPost_Staging.Tracking`
)

SELECT
    So.table_name,
    So.source_table,
    DATE(So.Source_max_date) AS Source_max_date,
    DATE(St.Staging_max_date) AS Staging_max_date,
    CURRENT_DATE() AS Date1,
    So.Source_pk_count,
    St.Staging_pk_count
FROM Sources AS So
LEFT JOIN Staging AS St
    ON So.table_name = St.table_name
ORDER BY table_name;
